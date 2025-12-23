const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const { spawn } = require('child_process'); 
const { promises: fs } = require('fs'); 
const path = require('path');
const fetch = require('node-fetch'); 

// --- Configuration ---
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY; 
const PORT = process.env.PORT || 10000;

const BUCKET_GENERATED = 'generated-content';
const BUCKET_LOGOS = 'logo-videos';
const BUCKET_MUSIC = 'music-tracks';

const POLLING_INTERVAL_MS = 5000; 
const STATUS_PENDING = 'PENDING'; 
const STATUS_IN_PROGRESS = 'PROCESSING_RENDER'; 
const STATUS_COMPLETED = 'RENDERING_COMPLETE'; 
const STATUS_FAILED = 'FAILED'; 

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

function buildPublicUrl(bucket, fileName) {
    if (!fileName) return null;
    if (fileName.startsWith('http')) return encodeURI(fileName);
    const cleanName = fileName.replace(/^\/+/, '');
    return encodeURI(`${SUPABASE_URL}/storage/v1/object/public/${bucket}/${cleanName}`);
}

async function downloadAsset(url, scriptId, assetName) {
    if (!url) return null;
    console.log(`[ASSET] Downloading ${assetName}: ${url}`);
    try {
        const response = await fetch(url);
        if (response.ok) {
            const ext = assetName === 'music' ? '.mp3' : '.mp4';
            const tempPath = path.join('/tmp', `${assetName}_${scriptId}_${Date.now()}${ext}`);
            const writer = require('fs').createWriteStream(tempPath);
            response.body.pipe(writer);
            return new Promise((resolve) => writer.on('finish', () => resolve(tempPath)));
        }
    } catch (e) {
        console.error(`[ASSET] Download Error: ${e.message}`);
    }
    return null;
}

async function processNextJob() {
    const { data: jobs } = await supabase.from('story_script').select('*').eq('status', STATUS_PENDING).limit(1);
    if (!jobs?.length) return;

    const job = jobs[0];
    const scriptId = job.id;
    
    try {
        await supabase.from('story_script').update({ status: STATUS_IN_PROGRESS }).eq('id', scriptId);
        console.log(`[DEBUG] Script Data: ${JSON.stringify(job.script_data)}`);

        // 1. Resolve Assets
        const { data: logoRow } = await supabase.from('logo_videos').select('video_url').order('created_at', { ascending: false }).limit(1).maybeSingle();
        const logoUrl = logoRow ? buildPublicUrl(BUCKET_LOGOS, logoRow.video_url) : null;

        let musicUrl = null;
        const sd = job.script_data || {};
        const musicRef = sd.background_music || sd.music;
        
        if (musicRef) {
            const { data: track } = await supabase.from('music_tracks').select('url, file_path')
                .or(`title.ilike.%${musicRef}%,file_path.ilike.%${musicRef}%`).limit(1).maybeSingle();
            if (track) musicUrl = track.url || buildPublicUrl(BUCKET_MUSIC, track.file_path);
        }

        const lPath = await downloadAsset(logoUrl, scriptId, 'logo');
        const mPath = await downloadAsset(musicUrl, scriptId, 'music');

        // 2. FFmpeg Command Construction
        const outPath = path.join('/tmp', `out_${scriptId}.mp4`);
        const duration = sd.total_duration || 10;
        
        let inputs = [];
        let filter = "";

        // SOURCE 0: Logo (if exists)
        if (lPath) {
            inputs.push('-i', lPath);
        }

        // SOURCE 1: Main background (Black color)
        inputs.push('-f', 'lavfi', '-i', `color=c=black:s=1280x720:d=${duration}`);

        // SOURCE 2: Audio (Music or Silence)
        if (mPath) {
            inputs.push('-i', mPath);
        } else {
            inputs.push('-f', 'lavfi', '-i', `anullsrc=r=44100:cl=stereo:d=${duration + 5}`);
        }

        // Complex Filter Logic
        if (lPath) {
            // Concatenate Logo (0:v) and Background (1:v)
            filter += "[0:v][1:v]concat=n=2:v=1:a=0[v_out];";
            // Map Audio from input 2 (Music/Silence)
            filter += "[2:a]atrim=0,afade=t=out:st="+(duration+duration/2)+":d=1[a_out]";
        } else {
            // No logo, just use Background (0:v)
            filter += "[0:v]copy[v_out];";
            // Map Audio from input 1 (Music/Silence)
            filter += "[1:a]atrim=0,afade=t=out:st="+(duration-1)+":d=1[a_out]";
        }

        const args = [
            ...inputs,
            '-filter_complex', filter,
            '-map', '[v_out]',
            '-map', '[a_out]',
            '-c:v', 'libx264',
            '-pix_fmt', 'yuv420p',
            '-preset', 'ultrafast',
            '-c:a', 'aac',
            '-shortest',
            '-y',
            outPath
        ];

        console.log(`[FFMPEG] Executing...`);
        await new Promise((resolve, reject) => {
            const proc = spawn('ffmpeg', args);
            proc.stderr.on('data', (data) => console.log(`[FFMPEG LOG] ${data.toString()}`));
            proc.on('close', (code) => code === 0 ? resolve() : reject(new Error(`FFmpeg Failed Code ${code}`)));
        });

        // 3. Upload Result
        const videoBuf = await fs.readFile(outPath);
        const fileName = `renders/${scriptId}_final.mp4`;
        await supabase.storage.from(BUCKET_GENERATED).upload(fileName, videoBuf, { contentType: 'video/mp4', upsert: true });

        const { data: pUrl } = supabase.storage.from(BUCKET_GENERATED).getPublicUrl(fileName);
        await supabase.from('story_script').update({ status: STATUS_COMPLETED, final_video_url: pUrl.publicUrl }).eq('id', scriptId);

        // Cleanup
        if (lPath) fs.unlink(lPath).catch(() => {});
        if (mPath) fs.unlink(mPath).catch(() => {});
        fs.unlink(outPath).catch(() => {});

    } catch (e) {
        console.error("Render Job Error:", e);
        await supabase.from('story_script').update({ status: STATUS_FAILED, error_message: e.message }).eq('id', scriptId);
    }
}

const app = express();
app.post(['/render', '/process'], (req, res) => res.sendStatus(202));
app.listen(PORT, () => {
    console.log(`Render Engine active on ${PORT}`);
    setInterval(processNextJob, POLLING_INTERVAL_MS);
});
