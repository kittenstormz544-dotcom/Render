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

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

/**
 * Ensures we have a full, valid Supabase URL
 */
function ensureFullUrl(input, bucket) {
    if (!input) return null;
    let str = String(input);
    
    // Fix the "Double URL" issue where paths are repeated
    if (str.includes('https://') && str.lastIndexOf('https://') > 0) {
        str = str.substring(str.lastIndexOf('https://'));
    }

    // If it's already a full URL, just clean it
    if (str.startsWith('http')) return encodeURI(decodeURI(str));

    // If it's just a filename, build the full Supabase URL
    return encodeURI(`${SUPABASE_URL}/storage/v1/object/public/${bucket}/${str.replace(/^\/+/, '')}`);
}

async function downloadAsset(url, scriptId, assetName) {
    if (!url) return null;
    console.log(`[ASSET] Attempting: ${assetName} -> ${url}`);
    try {
        const response = await fetch(url);
        if (response.ok) {
            const ext = assetName === 'music' ? '.mp3' : '.mp4';
            const tempPath = path.join('/tmp', `${assetName}_${scriptId}_${Date.now()}${ext}`);
            const writer = require('fs').createWriteStream(tempPath);
            response.body.pipe(writer);
            return new Promise((resolve) => {
                writer.on('finish', () => resolve(tempPath));
                writer.on('error', () => resolve(null));
            });
        }
        console.error(`[ASSET] ${assetName} Download Failed (HTTP ${response.status})`);
    } catch (e) {
        console.error(`[ASSET] ${assetName} Error: ${e.message}`);
    }
    return null;
}

async function processNextJob() {
    let jobs;
    try {
        const result = await supabase.from('story_script').select('*').eq('status', 'PENDING').limit(1);
        jobs = result.data;
    } catch (err) { return; }

    if (!jobs?.length) return;

    const job = jobs[0];
    const scriptId = job.id;
    
    try {
        await supabase.from('story_script').update({ status: 'PROCESSING_RENDER', progress_percentage: "15" }).eq('id', scriptId);

        let sd = job.script_data;
        if (typeof sd === 'string') {
            try { sd = JSON.parse(sd); } catch(e) { sd = {}; }
        }
        sd = sd || {};

        console.log(`[JOB] Starting ID: ${scriptId}`);

        // 1. Resolve Assets
        const { data: logoRow } = await supabase.from('logo_videos').select('video_url').order('created_at', { ascending: false }).limit(1).maybeSingle();
        
        // Try to get logo from table or from script_data
        const logoUrl = ensureFullUrl(logoRow?.video_url || sd.logo_video, BUCKET_LOGOS);
        const musicUrl = ensureFullUrl(sd.audio_engine?.moodTrack?.url, BUCKET_MUSIC);
        
        const lPath = await downloadAsset(logoUrl, scriptId, 'logo');
        const mPath = await downloadAsset(musicUrl, scriptId, 'music');

        // 2. FFmpeg Command Construction
        const outPath = path.join('/tmp', `final_render_${scriptId}.mp4`);
        const duration = sd.total_duration || 10;
        
        let inputs = [];
        let filter = "";

        // Build Inputs array dynamically based on what actually downloaded
        if (lPath) inputs.push('-i', lPath); // Input 0 (if exists)
        
        // Background color source
        inputs.push('-f', 'lavfi', '-i', `color=c=black:s=1280x720:r=25:d=${duration}`);
        
        // Audio source (Music or Silence)
        if (mPath) {
            inputs.push('-i', mPath);
        } else {
            inputs.push('-f', 'lavfi', '-i', `anullsrc=r=44100:cl=stereo:d=${duration + 5}`);
        }

        // Filter Logic - Correcting the indexing dynamically
        if (lPath) {
            // Logo exists at [0], Background at [1], Audio at [2]
            filter += "[0:v]scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2,setsar=1,fps=25,format=yuv420p[v0];";
            filter += "[1:v]fps=25,format=yuv420p[v1];";
            filter += "[v0][v1]concat=n=2:v=1:a=0[v_out];";
            filter += "[2:a]atrim=0,afade=t=out:st="+(duration+4)+":d=1[a_out]";
        } else {
            // No logo. Background at [0], Audio at [1]
            filter += "[0:v]fps=25,format=yuv420p[v_out];";
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

        console.log(`[FFMPEG] Starting Render...`);
        await new Promise((resolve, reject) => {
            const proc = spawn('ffmpeg', args);
            // Log output to help debugging
            proc.stderr.on('data', (data) => console.log(`[FFMPEG LOG] ${data.toString()}`));
            proc.on('close', (code) => code === 0 ? resolve() : reject(new Error(`FFmpeg Failed Code ${code}`)));
        });

        // 3. Upload & Finish
        const videoBuf = await fs.readFile(outPath);
        const fileName = `renders/${scriptId}_final_${Date.now()}.mp4`;
        await supabase.storage.from(BUCKET_GENERATED).upload(fileName, videoBuf, { contentType: 'video/mp4', upsert: true });

        const { data: pUrl } = supabase.storage.from(BUCKET_GENERATED).getPublicUrl(fileName);
        await supabase.from('story_script').update({ 
            status: 'COMPLETED', 
            final_video_url: pUrl.publicUrl, 
            progress_percentage: "100" 
        }).eq('id', scriptId);

        console.log(`[JOB] Finished Render for ID: ${scriptId}`);

        // Cleanup temporary files
        if (lPath) await fs.unlink(lPath).catch(() => {});
        if (mPath) await fs.unlink(mPath).catch(() => {});
        await fs.unlink(outPath).catch(() => {});

    } catch (e) {
        console.error("Render Job Error:", e);
        await supabase.from('story_script').update({ status: 'FAILED', error_message: e.message }).eq('id', scriptId);
    }
}

const app = express();
app.use(express.json());
app.post(['/render', '/process'], (req, res) => res.sendStatus(202));

app.listen(PORT, () => {
    console.log(`Render Engine active on port ${PORT}`);
    // Start polling the database for jobs
    setInterval(processNextJob, 5000);
});
