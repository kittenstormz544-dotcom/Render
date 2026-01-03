const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const { spawn, exec } = require('child_process'); 
const { promises: fs } = require('fs'); 
const path = require('path');
const fetch = require('node-fetch'); 

// --- CONFIGURATION ---
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY; 
const PORT = process.env.PORT || 10000;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

// --- UTILITIES ---
function ensureFullUrl(input, bucket = 'generated-content') {
    if (!input || input === "null" || input === "") return null;
    let str = String(input);
    if (str.startsWith('http')) return encodeURI(decodeURI(str));
    return `${SUPABASE_URL}/storage/v1/object/public/${bucket}/${str.replace(/^\/+/, '')}`;
}

// Check if a video actually has an audio stream
function hasAudio(filePath) {
    return new Promise((resolve) => {
        exec(`ffprobe -v error -show_streams -select_streams a "${filePath}"`, (err, stdout) => {
            if (err) return resolve(false);
            resolve(!!stdout && stdout.trim().length > 0);
        });
    });
}

async function runFFmpeg(args) {
    return new Promise((resolve, reject) => {
        const proc = spawn('ffmpeg', args);
        let errStr = '';
        proc.stderr.on('data', (d) => errStr += d.toString());
        proc.on('close', (code) => code === 0 ? resolve() : reject(new Error(errStr)));
    });
}

// --- CORE LOGIC ---
async function processJob(scriptId) {
    try {
        console.log(`[MASTER RENDER] Processing Script: ${scriptId}`);
        await supabase.from('story_script').update({ status: 'PROCESSING_RENDER', progress_percentage: "50" }).eq('id', scriptId);

        const { data: job } = await supabase.from('story_script').select('*').eq('id', scriptId).single();
        let sd = typeof job.script_data === 'string' ? JSON.parse(job.script_data) : job.script_data;
        
        const scenes = sd.scenes || [];
        const processedPaths = [];

        for (let i = 0; i < scenes.length; i++) {
            const scene = scenes[i];
            if (!scene.video_url) continue;

            console.log(`[SCENE ${i}] Verifying stream...`);
            const videoUrl = ensureFullUrl(scene.video_url);
            const res = await fetch(videoUrl);
            const tempIn = path.join('/tmp', `in_${i}_${scriptId}.mp4`);
            const outTs = path.join('/tmp', `scene_${i}_${scriptId}.ts`);
            
            await fs.writeFile(tempIn, Buffer.from(await res.arrayBuffer()));

            // FIXED: Check if audio exists so we don't crash the filtergraph
            const audioExists = await hasAudio(tempIn);
            console.log(`[SCENE ${i}] Audio present: ${audioExists}`);

            let filterComplex = '';
            let mapArgs = [];

            if (audioExists) {
                // Mix existing audio with silent baseline for safety
                filterComplex = '[0:v]scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2,setsar=1,fps=24[v];[0:a][1:a]amix=inputs=2:duration=first[a]';
                mapArgs = ['-map', '[v]', '-map', '[a]'];
            } else {
                // No audio in file? Use the silent generator purely
                filterComplex = '[0:v]scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2,setsar=1,fps=24[v]';
                mapArgs = ['-map', '[v]', '-map', '1:a'];
            }

            await runFFmpeg([
                '-i', tempIn,
                '-f', 'lavfi', '-i', 'anullsrc=r=44100:cl=stereo',
                '-filter_complex', filterComplex,
                ...mapArgs,
                '-c:v', 'libx264', '-preset', 'ultrafast',
                '-c:a', 'aac', '-ar', '44100',
                '-f', 'mpegts', '-y', outTs
            ]);
            processedPaths.push(outTs);
        }

        if (processedPaths.length === 0) throw new Error("No scenes were processed.");

        const finalPath = path.join('/tmp', `final_${scriptId}.mp4`);
        const concatString = `concat:${processedPaths.join('|')}`;

        console.log(`[STITCHING] Final assembly...`);
        await runFFmpeg([
            '-i', concatString,
            '-c', 'copy', 
            '-bsf:a', 'aac_adtstoasc', 
            '-y', finalPath
        ]);

        const finalFileName = `movie_final_${scriptId}_${Date.now()}.mp4`;
        await supabase.storage.from('generated-content').upload(finalFileName, await fs.readFile(finalPath), { contentType: 'video/mp4' });
        const { data: pUrl } = supabase.storage.from('generated-content').getPublicUrl(finalFileName);
        
        await supabase.from('story_script').update({ 
            status: 'COMPLETED', 
            final_video_url: pUrl.publicUrl, 
            progress_percentage: "100" 
        }).eq('id', scriptId);

        console.log(`[SUCCESS] Master Movie ready: ${pUrl.publicUrl}`);

        for (const p of processedPaths) await fs.unlink(p).catch(() => {});
        await fs.unlink(finalPath).catch(() => {});

    } catch (e) {
        console.error(`[RENDER ERROR]`, e.message);
        await supabase.from('story_script').update({ status: 'FAILED', error_message: e.message }).eq('id', scriptId);
    }
}

// --- SERVER SETUP ---
const app = express().use(express.json());

app.post(['/render', '/process'], (req, res) => {
    const id = req.body?.id || req.body?.record?.id || req.body?.scriptId;
    res.status(202).json({ status: "accepted" });
    if (id) processJob(id);
});

app.listen(PORT, () => console.log(`Render Engine Online on port ${PORT}`));
