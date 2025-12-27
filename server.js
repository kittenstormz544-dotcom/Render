const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const { spawn } = require('child_process'); 
const { promises: fs } = require('fs'); 
const path = require('path');
const fetch = require('node-fetch'); 

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY; 
const PORT = process.env.PORT || 10000;

const BUCKET_GENERATED = 'generated-content';
const BUCKET_LOGOS = 'logo-videos';
const BUCKET_MUSIC = 'music-tracks';

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

function ensureFullUrl(input, bucket) {
    if (!input) return null;
    let str = String(input);
    if (str.includes('https://') && str.lastIndexOf('https://') > 0) {
        str = str.substring(str.lastIndexOf('https://'));
    }
    if (str.startsWith('http')) return encodeURI(decodeURI(str));
    return encodeURI(`${SUPABASE_URL}/storage/v1/object/public/${bucket}/${str.replace(/^\/+/, '')}`);
}

async function downloadAsset(url, scriptId, assetName) {
    if (!url) return null;
    try {
        const response = await fetch(url);
        if (response.ok) {
            const ext = assetName === 'music' ? '.mp3' : '.mp4';
            const tempPath = path.join('/tmp', `${assetName}_${scriptId}_${Date.now()}${ext}`);
            const buffer = await response.arrayBuffer();
            await fs.writeFile(tempPath, Buffer.from(buffer));
            console.log(`[ASSET] Downloaded ${assetName} to ${tempPath}`);
            return tempPath;
        }
    } catch (e) { console.error(`[ASSET] ${assetName} Error: ${e.message}`); }
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
        console.log(`[JOB] Processing ID: ${scriptId}`);
        // Immediately set to PROCESSING so other instances don't grab it
        await supabase.from('story_script').update({ status: 'PROCESSING_RENDER', progress_percentage: "20" }).eq('id', scriptId);

        let sd = job.script_data;
        if (typeof sd === 'string') { try { sd = JSON.parse(sd); } catch(e) { sd = {}; } }
        sd = sd || {};

        const { data: logoRow } = await supabase.from('logo_videos').select('video_url').order('created_at', { ascending: false }).limit(1).maybeSingle();
        const logoUrl = ensureFullUrl(logoRow?.video_url || sd.logo_video, BUCKET_LOGOS);
        const musicUrl = ensureFullUrl(sd.audio_engine?.moodTrack?.url, BUCKET_MUSIC);
        
        const lPath = await downloadAsset(logoUrl, scriptId, 'logo');
        const mPath = await downloadAsset(musicUrl, scriptId, 'music');

        const outPath = path.join('/tmp', `render_${scriptId}.mp4`);
        const duration = sd.total_duration || 10;
        
        let inputs = [];
        let filter = "";
        if (lPath) inputs.push('-i', lPath);
        inputs.push('-f', 'lavfi', '-i', `color=c=black:s=1280x720:r=25:d=${duration}`);
        if (mPath) inputs.push('-i', mPath);
        else inputs.push('-f', 'lavfi', '-i', `anullsrc=r=44100:cl=stereo:d=${duration + 5}`);

        if (lPath) {
            filter += "[0:v]scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2,setsar=1,fps=25,format=yuv420p[v0];";
            filter += "[1:v]fps=25,format=yuv420p[v1];";
            filter += "[v0][v1]concat=n=2:v=1:a=0[v_out];";
            filter += "[2:a]atrim=0,afade=t=out:st="+(duration+4)+":d=1[a_out]";
        } else {
            filter += "[0:v]fps=25,format=yuv420p[v_out];";
            filter += "[1:a]atrim=0,afade=t=out:st="+(duration-1)+":d=1[a_out]";
        }

        const args = [...inputs, '-filter_complex', filter, '-map', '[v_out]', '-map', '[a_out]', '-c:v', 'libx264', '-pix_fmt', 'yuv420p', '-preset', 'ultrafast', '-c:a', 'aac', '-shortest', '-y', outPath];

        console.log(`[FFMPEG] Starting render for ${scriptId}...`);
        await new Promise((resolve, reject) => {
            const proc = spawn('ffmpeg', args);
            proc.on('close', (code) => code === 0 ? resolve() : reject(new Error(`FFmpeg error ${code}`)));
        });

        console.log(`[STORAGE] Uploading to bucket: ${BUCKET_GENERATED}...`);
        const videoBuf = await fs.readFile(outPath);
        const fileName = `${scriptId}_final_${Date.now()}.mp4`;
        
        const { error: uploadError } = await supabase.storage.from(BUCKET_GENERATED).upload(fileName, videoBuf, { contentType: 'video/mp4', upsert: true });
        
        if (uploadError) {
            console.error(`[STORAGE ERROR] ${uploadError.message}`);
            throw new Error(`Upload failed: ${uploadError.message}`);
        }

        console.log(`[DB] Marking job as COMPLETED...`);
        const { data: pUrl } = supabase.storage.from(BUCKET_GENERATED).getPublicUrl(fileName);
        
        const { error: updateError } = await supabase.from('story_script').update({ 
            status: 'COMPLETED', 
            final_video_url: pUrl.publicUrl, 
            progress_percentage: "100" 
        }).eq('id', scriptId);
        
        if (updateError) {
            console.error(`[DB ERROR] ${updateError.message}`);
            throw new Error(`DB Update failed: ${updateError.message}`);
        }

        console.log(`[SUCCESS] Job ${scriptId} finished successfully.`);
        
        // Cleanup temp files
        if (lPath) await fs.unlink(lPath).catch(() => {});
        if (mPath) await fs.unlink(mPath).catch(() => {});
        await fs.unlink(outPath).catch(() => {});

    } catch (e) {
        console.error(`[FATAL ERROR] Job ${scriptId}:`, e.message);
        // Try to report the failure to Supabase so it doesn't stay in "Processing" forever
        await supabase.from('story_script').update({ 
            status: 'FAILED', 
            error_message: e.message 
        }).eq('id', scriptId);
    }
}

const app = express();
app.use(express.json());

// Handle the trigger from Supabase functions
app.post(['/render', '/process'], (req, res) => {
    res.status(202).json({ status: "queued", message: "Render engine is processing jobs" });
});

app.listen(PORT, () => {
    console.log(`Render Engine active on port ${PORT}`);
    // Check for jobs every 5 seconds
    setInterval(processNextJob, 5000);
});
