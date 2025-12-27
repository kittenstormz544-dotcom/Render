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
            const tempPath = path.join('/tmp', `${assetName}_${scriptId}_${Date.now()}.mp4`);
            const buffer = await response.arrayBuffer();
            await fs.writeFile(tempPath, Buffer.from(buffer));
            console.log(`[DOWNLOAD] Success: ${assetName}`);
            return tempPath;
        }
    } catch (e) { console.error(`[DOWNLOAD ERROR] ${assetName}: ${e.message}`); }
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
        console.log(`[JOB] Starting Stitching for ID: ${scriptId}`);
        
        // SAFETY CHECK: Check if script_data exists
        if (!job.script_data) {
            throw new Error("The 'script_data' column is empty (null). The AI assistant hasn't saved the scene data yet.");
        }

        await supabase.from('story_script').update({ status: 'PROCESSING_RENDER', progress_percentage: "20" }).eq('id', scriptId);

        let sd = job.script_data;
        if (typeof sd === 'string') { try { sd = JSON.parse(sd); } catch(e) { sd = {}; } }
        
        const videoPaths = [];
        
        // 1. Download Logo
        const logoUrl = ensureFullUrl(sd.logo_video, BUCKET_LOGOS);
        const lPath = await downloadAsset(logoUrl, scriptId, 'logo');
        if (lPath) videoPaths.push(lPath);

        // 2. Download Scenes
        if (sd.scenes && Array.isArray(sd.scenes)) {
            for (let i = 0; i < sd.scenes.length; i++) {
                const scene = sd.scenes[i];
                if (scene.video_url) {
                    const sPath = await downloadAsset(scene.video_url, 'scene-videos'); // Adjust bucket if needed
                    if (sPath) videoPaths.push(sPath);
                }
            }
        }

        // 3. Download Music
        const musicUrl = ensureFullUrl(sd.audio_engine?.moodTrack?.url, BUCKET_MUSIC);
        const mPath = await downloadAsset(musicUrl, scriptId, 'music');

        if (videoPaths.length === 0) throw new Error("No video files found in script_data. Check if scene video_urls were saved correctly.");

        const outPath = path.join('/tmp', `output_${scriptId}.mp4`);
        
        let filter = "";
        let inputArgs = [];
        videoPaths.forEach((p, idx) => {
            inputArgs.push('-i', p);
            filter += `[${idx}:v]scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2,setsar=1,fps=25,format=yuv420p[v${idx}];`;
        });

        const videoNodes = videoPaths.map((_, i) => `[v${i}]`).join('');
        filter += `${videoNodes}concat=n=${videoPaths.length}:v=1:a=0[v_out]`;

        let args = [...inputArgs];
        if (mPath) {
            args.push('-i', mPath);
            const musicIdx = videoPaths.length;
            args.push('-filter_complex', filter, '-map', '[v_out]', '-map', `${musicIdx}:a`, '-c:v', 'libx264', '-preset', 'ultrafast', '-c:a', 'aac', '-shortest', '-y', outPath);
        } else {
            args.push('-filter_complex', filter, '-map', '[v_out]', '-c:v', 'libx264', '-preset', 'ultrafast', '-y', outPath);
        }

        await new Promise((resolve, reject) => {
            const proc = spawn('ffmpeg', args);
            proc.on('close', (code) => code === 0 ? resolve() : reject(new Error(`FFmpeg error ${code}`)));
        });

        const videoBuf = await fs.readFile(outPath);
        const fileName = `final_movie_${scriptId}_${Date.now()}.mp4`;
        await supabase.storage.from(BUCKET_GENERATED).upload(fileName, videoBuf, { contentType: 'video/mp4' });

        const { data: pUrl } = supabase.storage.from(BUCKET_GENERATED).getPublicUrl(fileName);
        await supabase.from('story_script').update({ 
            status: 'COMPLETED', 
            final_video_url: pUrl.publicUrl, 
            progress_percentage: "100" 
        }).eq('id', scriptId);

        console.log(`[SUCCESS] Movie complete: ${scriptId}`);

        // Cleanup
        for (const p of videoPaths) await fs.unlink(p).catch(() => {});
        if (mPath) await fs.unlink(mPath).catch(() => {});
        await fs.unlink(outPath).catch(() => {});

    } catch (e) {
        console.error(`[ERROR] ${e.message}`);
        await supabase.from('story_script').update({ 
            status: 'FAILED', 
            error_message: e.message 
        }).eq('id', scriptId);
    }
}

const app = express();
app.listen(PORT, () => {
    console.log(`Render Engine Online on Port ${PORT}`);
    setInterval(processNextJob, 5000);
});
