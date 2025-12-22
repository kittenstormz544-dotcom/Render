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

        // 1. Resolve Logo
        const { data: logoRow } = await supabase.from('logo_videos').select('video_url').order('created_at', { ascending: false }).limit(1).maybeSingle();
        const logoUrl = logoRow ? buildPublicUrl(BUCKET_LOGOS, logoRow.video_url) : null;

        // 2. Resolve Music
        let musicUrl = null;
        const sd = job.script_data || {};
        const musicRef = sd.background_music || sd.music || job.background_music_url;
        
        if (musicRef) {
            const { data: track } = await supabase.from('music_tracks').select('url, file_path')
                .or(`title.ilike.%${musicRef}%,file_path.ilike.%${musicRef}%`).limit(1).maybeSingle();
            if (track) musicUrl = track.url || buildPublicUrl(BUCKET_MUSIC, track.file_path);
        }

        // If no music found by title, try picking ANY track
        if (!musicUrl) {
            const { data: anyTrack } = await supabase.from('music_tracks').select('url, file_path').limit(1).maybeSingle();
            if (anyTrack) musicUrl = anyTrack.url || buildPublicUrl(BUCKET_MUSIC, anyTrack.file_path);
        }

        const lPath = await downloadAsset(logoUrl, scriptId, 'logo');
        const mPath = await downloadAsset(musicUrl, scriptId, 'music');

        // 3. FFmpeg with Silence Fallback
        const outPath = path.join('/tmp', `out_${scriptId}.mp4`);
        const duration = sd.total_duration || 10;
        
        let args = [];
        // Audio input: Use mPath if exists, otherwise generate silence
        const audioInput = mPath ? ['-i', mPath] : ['-f', 'lavfi', '-i', 'anullsrc=r=44100:cl=stereo'];
        const audioMap = mPath ? '2:a' : '2:a'; // Map from the audio input index

        if (lPath) {
            args = [
                '-i', lPath,
                '-f', 'lavfi', '-i', `color=c=black:s=1280x720:d=${duration}`,
                ...audioInput,
                '-filter_complex', '[0:v][1:v]concat=n=2:v=1:a=0[vv]',
                '-map', '[vv]', '-map', audioMap, '-c:v', 'libx264', '-pix_fmt', 'yuv420p', '-c:a', 'aac', '-shortest', '-y', outPath
            ];
        } else {
            args = [
                '-f', 'lavfi', '-i', `color=c=black:s=1280x720:d=${duration}`,
                ...audioInput,
                '-map', '0:v', '-map', '1:a', '-c:v', 'libx264', '-pix_fmt', 'yuv420p', '-c:a', 'aac', '-shortest', '-y', outPath
            ];
        }

        await new Promise((resolve, reject) => {
            const proc = spawn('ffmpeg', args.flat());
            proc.on('close', (code) => code === 0 ? resolve() : reject(new Error("FFmpeg failed")));
        });

        // 4. Upload
        const videoBuf = await fs.readFile(outPath);
        const fileName = `renders/${scriptId}_final.mp4`;
        await supabase.storage.from(BUCKET_GENERATED).upload(fileName, videoBuf, { contentType: 'video/mp4', upsert: true });

        const { data: pUrl } = supabase.storage.from(BUCKET_GENERATED).getPublicUrl(fileName);
        await supabase.from('story_script').update({ status: STATUS_COMPLETED, final_video_url: pUrl.publicUrl }).eq('id', scriptId);

    } catch (e) {
        console.error("Render Job Error:", e);
        await supabase.from('story_script').update({ status: STATUS_FAILED, error_message: e.message }).eq('id', scriptId);
    }
}

const app = express();
app.post(['/render', '/process'], (req, res) => res.sendStatus(202));
app.listen(PORT, () => setInterval(processNextJob, POLLING_INTERVAL_MS));
