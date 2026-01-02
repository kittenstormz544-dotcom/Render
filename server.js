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

function isVideoUrl(url) {
    if (!url) return false;
    const cleanUrl = url.split('?')[0].toLowerCase();
    // If it's a known image format, it's definitely NOT a video
    if (cleanUrl.endsWith('.png') || cleanUrl.endsWith('.jpg') || cleanUrl.endsWith('.jpeg') || cleanUrl.endsWith('.webp')) {
        return false;
    }
    // If it has a video extension OR if it's a large file from ScVideo, we treat it as video
    return cleanUrl.endsWith('.mp4') || cleanUrl.endsWith('.webm') || cleanUrl.endsWith('.mov') || !cleanUrl.includes('.');
}

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
    if (!activeJobs.has(scriptId)) activeJobs.add(scriptId); else return;

    try {
        console.log(`[DIAGNOSTICS] Starting Render for ID: ${scriptId}`);
        await supabase.from('story_script').update({ status: 'PROCESSING_RENDER', progress_percentage: "40" }).eq('id', scriptId);

        let job = null;
        let retries = 0;
        
        while (retries < 150) { 
            const { data } = await supabase.from('story_script').select('*').eq('id', scriptId).single();
            let parsed = typeof data.script_data === 'string' ? JSON.parse(data.script_data) : data.script_data;
            
            const scenes = parsed?.scenes || [];
            const readyVideoClips = scenes.filter(s => isVideoUrl(s.video_url));
            
            // LOGGING THE ACTUAL URLS FOR YOU TO SEE IN RENDER
            scenes.forEach((s, i) => {
                console.log(`[SCENE ${i}] URL: ${s.video_url || 'EMPTY'} | IS_VIDEO: ${isVideoUrl(s.video_url)}`);
            });

            if (readyVideoClips.length > 0 && readyVideoClips.length === scenes.length) {
                job = { ...data, parsed_data: parsed };
                break;
            }
            
            console.log(`[WAITING] Found ${readyVideoClips.length}/${scenes.length} real videos. Checking again in 15s...`);
            await new Promise(r => setTimeout(r, 15000));
            retries++;
        }

        if (!job) throw new Error("Timeout: ScVideo did not finish the video clips in time.");

        const rawPaths = [];
        const sd = job.parsed_data;
        const lPath = await downloadAsset(ensureFullUrl(sd?.logo_video, BUCKET_LOGOS), scriptId, 'logo');
        if (lPath) rawPaths.push(lPath);

        const videoScenes = sd.scenes.filter(s => isVideoUrl(s.video_url));
        for (let i = 0; i < videoScenes.length; i++) {
            const sPath = await downloadAsset(ensureFullUrl(videoScenes[i].video_url, BUCKET_SCENES), scriptId, `scene_${i}`);
            if (sPath) rawPaths.push(sPath);
        }

        const mPath = await downloadAsset(ensureFullUrl(sd?.audio_engine?.moodTrack?.url, BUCKET_MUSIC), scriptId, 'music', '.mp3');

        const processedPaths = [];
        for (let i = 0; i < rawPaths.length; i++) {
            const outP = rawPaths[i] + '.ts';
            const audioFound = await hasAudio(rawPaths[i]);
            let filter = `[0:v]scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2,setsar=1,fps=25,format=yuv420p[v]`;
            let fArgs = ['-i', rawPaths[i], '-f', 'lavfi', '-i', 'anullsrc=r=44100:cl=stereo'];
            if (audioFound) {
                fArgs.push('-filter_complex', `${filter};[0:a][1:a]amix=inputs=2:duration=first[a]`, '-map', '[v]', '-map', '[a]');
            } else {
                fArgs.push('-filter_complex', filter, '-map', '[v]', '-map', '1:a');
            }
            fArgs.push('-c:v', 'libx264', '-preset', 'ultrafast', '-c:a', 'aac', '-shortest', '-y', outP);
            await runFFmpeg(fArgs);
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

        const finalFileName = `story_${scriptId}_${Date.now()}.mp4`;
        await supabase.storage.from(BUCKET_GENERATED).upload(finalFileName, await fs.readFile(outPath), { contentType: 'video/mp4' });
        const { data: pUrl } = supabase.storage.from(BUCKET_GENERATED).getPublicUrl(finalFileName);
        await supabase.from('story_script').update({ status: 'COMPLETED', final_video_url: pUrl.publicUrl, progress_percentage: "100" }).eq('id', scriptId);

        for (const p of [...rawPaths, ...processedPaths, mPath, outPath].filter(Boolean)) await fs.unlink(p).catch(() => {});
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
app.listen(PORT, () => console.log(`Render Engine Online`));
