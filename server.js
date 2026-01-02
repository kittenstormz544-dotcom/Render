const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const { spawn, exec } = require('child_process'); 
const { promises: fs } = require('fs'); 
const path = require('path');
const fetch = require('node-fetch'); 

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY; 
const PORT = process.env.PORT || 10000;

const BUCKET_GENERATED = 'generated-content';
const BUCKET_LOGOS = 'logo-videos';
const BUCKET_MUSIC = 'music-tracks';
const BUCKET_SCENES = 'generated-content'; 

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
const activeJobs = new Set();

function ensureFullUrl(input, bucket) {
    if (!input || input === "null" || input === "") return null;
    let str = String(input);
    if (str.startsWith('http')) return encodeURI(decodeURI(str));
    return `${SUPABASE_URL}/storage/v1/object/public/${bucket}/${str.replace(/^\/+/, '')}`;
}

async function downloadAsset(url, scriptId, assetName, ext = '.mp4') {
    if (!url) return null;
    try {
        const response = await fetch(url);
        if (response.ok) {
            const tempPath = path.join('/tmp', `${assetName}_${scriptId}_${Date.now()}${ext}`);
            const buffer = await response.arrayBuffer();
            await fs.writeFile(tempPath, Buffer.from(buffer));
            console.log(`[DOWNLOAD] Success: ${assetName}`);
            return tempPath;
        }
    } catch (e) { console.error(`[DOWNLOAD ERROR] ${assetName}: ${e.message}`); }
    return null;
}

function hasAudio(filePath) {
    return new Promise((resolve) => {
        exec(`ffprobe -v error -show_streams -select_streams a "${filePath}"`, (err, stdout) => {
            resolve(!!stdout && stdout.trim().length > 0);
        });
    });
}

function runFFmpeg(args) {
    return new Promise((resolve, reject) => {
        const proc = spawn('ffmpeg', args);
        let errData = '';
        proc.stderr.on('data', (data) => { errData += data.toString(); });
        proc.on('close', (code) => {
            if (code === 0) resolve();
            else {
                console.error('[FFMPEG ERROR DETAILS]:', errData);
                reject(new Error(`FFmpeg exit code ${code}`));
            }
        });
    });
}

async function processJob(scriptId) {
    if (!scriptId || activeJobs.has(scriptId)) return;
    activeJobs.add(scriptId);

    try {
        const { data: initialJob } = await supabase.from('story_script').select('*').eq('id', scriptId).single();
        if (!initialJob) throw new Error("Job not found");

        await supabase.from('story_script').update({ status: 'PROCESSING_RENDER', progress_percentage: "40" }).eq('id', scriptId);

        let job = initialJob;
        let retries = 0;
        // Wait up to 10 minutes (100 * 6s) for AI to finish ALL scenes
        while (retries < 100) { 
            const { data } = await supabase.from('story_script').select('*').eq('id', scriptId).single();
            let parsed = typeof data.script_data === 'string' ? JSON.parse(data.script_data) : data.script_data;
            
            // Check if we have scenes and if they ALL have URLs
            if (parsed && parsed.scenes && parsed.scenes.length > 1 && parsed.scenes.every(s => s.video_url)) {
                job = { ...data, parsed_data: parsed };
                console.log(`[RENDER READY] Found ${parsed.scenes.length} scenes. Starting render...`);
                break;
            }
            console.log(`[WAITING] AI is still generating scenes (current count: ${parsed?.scenes?.filter(s => s.video_url).length || 0})...`);
            await new Promise(r => setTimeout(r, 6000));
            retries++;
        }

        const rawPaths = [];
        const sd = job.parsed_data;
        
        const lPath = await downloadAsset(ensureFullUrl(sd?.logo_video, BUCKET_LOGOS), scriptId, 'logo');
        if (lPath) rawPaths.push(lPath);

        for (let i = 0; i < (sd.scenes?.length || 0); i++) {
            const sPath = await downloadAsset(ensureFullUrl(sd.scenes[i].video_url, BUCKET_SCENES), scriptId, `scene_${i}`);
            if (sPath) rawPaths.push(sPath);
        }

        const mUrl = ensureFullUrl(sd?.audio_engine?.moodTrack?.url, BUCKET_MUSIC);
        const mPath = await downloadAsset(mUrl, scriptId, 'music', '.mp3');

        const processedPaths = [];
        for (let i = 0; i < rawPaths.length; i++) {
            const outP = rawPaths[i] + '.ts';
            const audioFound = await hasAudio(rawPaths[i]);
            
            let ffmpegArgs = [
                '-i', rawPaths[i],
                '-f', 'lavfi', '-i', 'anullsrc=r=44100:cl=stereo'
            ];

            if (audioFound) {
                ffmpegArgs.push('-filter_complex', `[0:v]scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2,setsar=1,fps=25,format=yuv420p[v];[0:a][1:a]amix=inputs=2:duration=first[a]`);
            } else {
                ffmpegArgs.push('-filter_complex', `[0:v]scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2,setsar=1,fps=25,format=yuv420p[v]`);
                ffmpegArgs.push('-map', '[v]', '-map', '1:a');
            }

            ffmpegArgs.push('-c:v', 'libx264', '-preset', 'ultrafast', '-c:a', 'aac', '-shortest', '-y', outP);
            await runFFmpeg(ffmpegArgs);
            processedPaths.push(outP);
        }

        const outPath = path.join('/tmp', `final_${scriptId}.mp4`);
        let concatStr = "";
        let inputArgs = [];
        processedPaths.forEach((p, idx) => {
            inputArgs.push('-i', p);
            concatStr += `[${idx}:v][${idx}:a]`;
        });
        concatStr += `concat=n=${processedPaths.length}:v=1:a=1[vv][aa]`;

        let finalArgs = [...inputArgs];
        if (mPath) {
            finalArgs.push('-i', mPath);
            finalArgs.push('-filter_complex', `${concatStr};[aa]volume=1.0[v1];[${processedPaths.length}:a]volume=0.3[v2];[v1][v2]amix=inputs=2:duration=first[fa]`, '-map', '[vv]', '-map', '[fa]');
        } else {
            finalArgs.push('-filter_complex', concatStr, '-map', '[vv]', '-map', '[aa]');
        }
        
        finalArgs.push('-c:v', 'libx264', '-preset', 'ultrafast', '-y', outPath);
        await runFFmpeg(finalArgs);

        const videoBuf = await fs.readFile(outPath);
        const finalFileName = `story_${scriptId}_${Date.now()}.mp4`;
        await supabase.storage.from(BUCKET_GENERATED).upload(finalFileName, videoBuf, { contentType: 'video/mp4' });

        const { data: pUrl } = supabase.storage.from(BUCKET_GENERATED).getPublicUrl(finalFileName);
        await supabase.from('story_script').update({ status: 'COMPLETED', final_video_url: pUrl.publicUrl, progress_percentage: "100" }).eq('id', scriptId);

        console.log(`[SUCCESS] Full Movie Uploaded: ${pUrl.publicUrl}`);

        for (const p of [...rawPaths, ...processedPaths]) await fs.unlink(p).catch(() => {});
        if (mPath) await fs.unlink(mPath).catch(() => {});
        await fs.unlink(outPath).catch(() => {});

    } catch (e) {
        console.error(`[ERROR]`, e.message);
        await supabase.from('story_script').update({ status: 'FAILED', error_message: e.message }).eq('id', scriptId);
    } finally {
        activeJobs.delete(scriptId);
    }
}

const app = express();
app.use(express.json());
app.post(['/render', '/process'], (req, res) => {
    const id = req.body?.id || req.body?.record?.id || req.body?.scriptId;
    res.status(202).json({ status: "accepted" });
    if (id) processJob(id);
});
app.listen(PORT, () => console.log(`Render Engine Online on Port ${PORT}`));
