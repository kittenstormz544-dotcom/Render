const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const { spawn } = require('child_process'); 
const { promises: fs } = require('fs'); 
const path = require('path');
const fetch = require('node-fetch'); 

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY; 
const PORT = process.env.PORT || 10000;
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

function cleanUrl(url) {
    if (!url || typeof url !== 'string') return null;
    let target = url;
    if (target.includes('https://') && target.lastIndexOf('https://') > 0) {
        target = target.substring(target.lastIndexOf('https://'));
    }
    return encodeURI(decodeURI(target));
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
            return new Promise((resolve) => {
                writer.on('finish', () => resolve(tempPath));
                writer.on('error', () => resolve(null));
            });
        }
    } catch (e) { console.error(`[ASSET] Error: ${e.message}`); }
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
        if (typeof sd === 'string') { try { sd = JSON.parse(sd); } catch(e) { sd = {}; } }
        sd = sd || {};

        console.log(`[JOB] Starting ID: ${scriptId}`);

        const { data: logoRow } = await supabase.from('logo_videos').select('video_url').order('created_at', { ascending: false }).limit(1).maybeSingle();
        const logoUrl = logoRow ? cleanUrl(logoRow.video_url) : null;
        const musicUrl = cleanUrl(sd.audio_engine?.moodTrack?.url);
        
        const lPath = await downloadAsset(logoUrl, scriptId, 'logo');
        const mPath = await downloadAsset(musicUrl, scriptId, 'music');

        const outPath = path.join('/tmp', `final_${scriptId}.mp4`);
        const duration = sd.total_duration || 10;
        
        let inputs = [];
        let filter = "";

        // Standardizing inputs to 1280x720, 25fps, yuv420p to prevent "Code 1" errors
        if (lPath) {
            inputs.push('-i', lPath); // [0:v]
            inputs.push('-f', 'lavfi', '-i', `color=c=black:s=1280x720:r=25:d=${duration}`); // [1:v]
            filter += "[0:v]scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2,setsar=1,fps=25,format=yuv420p[v0];";
            filter += "[1:v]fps=25,format=yuv420p[v1];";
            filter += "[v0][v1]concat=n=2:v=1:a=0[v_out];";
        } else {
            inputs.push('-f', 'lavfi', '-i', `color=c=black:s=1280x720:r=25:d=${duration}`);
            filter += "[0:v]fps=25,format=yuv420p[v_out];";
        }

        if (mPath) {
            inputs.push('-i', mPath);
            const aIdx = lPath ? 2 : 1;
            filter += `[${aIdx}:a]atrim=0:${duration + 5},afade=t=out:st=${duration + 4}:d=1[a_out]`;
        } else {
            inputs.push('-f', 'lavfi', '-i', `anullsrc=r=44100:cl=stereo:d=${duration + 5}`);
            const aIdx = lPath ? 2 : 1;
            filter += `[${aIdx}:a]copy[a_out]`;
        }

        const args = [...inputs, '-filter_complex', filter, '-map', '[v_out]', '-map', '[a_out]', '-c:v', 'libx264', '-pix_fmt', 'yuv420p', '-preset', 'ultrafast', '-c:a', 'aac', '-shortest', '-y', outPath];

        await new Promise((resolve, reject) => {
            const proc = spawn('ffmpeg', args);
            // This captures the ACTUAL error from ffmpeg
            proc.stderr.on('data', (data) => console.log(`[FFMPEG DEBUG] ${data.toString()}`));
            proc.on('close', (code) => code === 0 ? resolve() : reject(new Error(`FFmpeg Failed: Code ${code}`)));
        });

        const videoBuf = await fs.readFile(outPath);
        const fileName = `renders/${scriptId}_final.mp4`;
        await supabase.storage.from('generated-content').upload(fileName, videoBuf, { contentType: 'video/mp4', upsert: true });

        const { data: pUrl } = supabase.storage.from('generated-content').getPublicUrl(fileName);
        await supabase.from('story_script').update({ status: 'COMPLETED', final_video_url: pUrl.publicUrl, progress_percentage: "100" }).eq('id', scriptId);

        if (lPath) await fs.unlink(lPath).catch(() => {});
        if (mPath) await fs.unlink(mPath).catch(() => {});
        await fs.unlink(outPath).catch(() => {});

    } catch (e) {
        console.error("Render Job Error:", e);
        await supabase.from('story_script').update({ status: 'FAILED', error_message: e.message }).eq('id', scriptId);
    }
}

const app = express();
app.listen(PORT, () => setInterval(processNextJob, 5000));
