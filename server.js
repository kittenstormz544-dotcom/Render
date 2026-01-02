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
const BUCKET_SCENES = 'generated-content'; 

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
const activeJobs = new Set();

function ensureFullUrl(input, bucket) {
    if (!input || input === "null" || input === "") return null;
    let str = String(input);
    if (str.startsWith('http')) {
        if (str.includes('https://') && str.lastIndexOf('https://') > 0) {
            str = str.substring(str.lastIndexOf('https://'));
        }
        return encodeURI(decodeURI(str));
    }
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
        let job = null;
        let retries = 0;
        while (retries < 30) {
            const { data } = await supabase.from('story_script').select('*').eq('id', scriptId).single();
            if (data && data.script_data) {
                let parsed = typeof data.script_data === 'string' ? JSON.parse(data.script_data) : data.script_data;
                if (parsed.scenes && parsed.scenes.length > 0) {
                    job = data;
                    job.parsed_data = parsed;
                    break;
                }
            }
            console.log(`[WAIT] Polling for scenes ID ${scriptId}... (${retries}/30)`);
            await new Promise(r => setTimeout(r, 4000));
            retries++;
        }

        if (!job) throw new Error("No scenes found in database");

        await supabase.from('story_script').update({ status: 'PROCESSING_RENDER', progress_percentage: "40" }).eq('id', scriptId);

        const rawPaths = [];
        const sd = job.parsed_data;
        
        const lPath = await downloadAsset(ensureFullUrl(sd?.logo_video, BUCKET_LOGOS), scriptId, 'logo');
        if (lPath) rawPaths.push(lPath);

        if (sd?.scenes) {
            for (let i = 0; i < sd.scenes.length; i++) {
                const sUrl = ensureFullUrl(sd.scenes[i].video_url, BUCKET_SCENES);
                const sPath = await downloadAsset(sUrl, scriptId, `scene_${i}`);
                if (sPath) rawPaths.push(sPath);
            }
        }

        const mUrl = ensureFullUrl(sd?.audio_engine?.moodTrack?.url, BUCKET_MUSIC);
        const mPath = await downloadAsset(mUrl, scriptId, 'music', '.mp3');

        // --- STEP 1: STANDARDIZE ---
        const processedPaths = [];
        console.log(`[FFMPEG] Standardizing ${rawPaths.length} clips...`);
        for (let i = 0; i < rawPaths.length; i++) {
            const outP = rawPaths[i] + '.ts';
            await runFFmpeg([
                '-i', rawPaths[i],
                '-f', 'lavfi', '-i', 'anullsrc=r=44100:cl=stereo',
                '-filter_complex', `[0:v]scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2,setsar=1,fps=25,format=yuv420p[v]`,
                '-map', '[v]', '-map', '1:a',
                '-c:v', 'libx264', '-preset', 'ultrafast', '-pix_fmt', 'yuv420p',
                '-c:a', 'aac', '-shortest',
                '-y', outP
            ]);
            processedPaths.push(outP);
        }

        // --- STEP 2: STITCH ---
        const outPath = path.join('/tmp', `final_output_${scriptId}.mp4`);
        let concatFilter = "";
        let inputArgs = [];
        processedPaths.forEach((p, idx) => {
            inputArgs.push('-i', p);
            concatFilter += `[${idx}:v][${idx}:a]`;
        });
        concatFilter += `concat=n=${processedPaths.length}:v=1:a=1[v][a]`;

        let finalArgs = [...inputArgs];
        if (mPath) {
            finalArgs.push('-i', mPath);
            finalArgs.push('-filter_complex', `${concatFilter};[a][${processedPaths.length}:a]amix=inputs=2:duration=first[fa]`, '-map', '[v]', '-map', '[fa]');
        } else {
            finalArgs.push('-filter_complex', concatFilter, '-map', '[v]', '-map', '[a]');
        }
        
        finalArgs.push('-c:v', 'libx264', '-preset', 'ultrafast', '-y', outPath);

        console.log(`[FFMPEG] Stitching final movie...`);
        await runFFmpeg(finalArgs);

        const videoBuf = await fs.readFile(outPath);
        const finalFileName = `final_${scriptId}_${Date.now()}.mp4`;
        await supabase.storage.from(BUCKET_GENERATED).upload(finalFileName, videoBuf, { contentType: 'video/mp4' });

        const { data: pUrl } = supabase.storage.from(BUCKET_GENERATED).getPublicUrl(finalFileName);
        await supabase.from('story_script').update({ 
            status: 'COMPLETED', 
            final_video_url: pUrl.publicUrl, 
            progress_percentage: "100" 
        }).eq('id', scriptId);

        console.log(`[SUCCESS] Movie: ${pUrl.publicUrl}`);

        for (const p of [...rawPaths, ...processedPaths]) await fs.unlink(p).catch(() => {});
        if (mPath) await fs.unlink(mPath).catch(() => {});
        await fs.unlink(outPath).catch(() => {});

    } catch (e) {
        console.error(`[FATAL] Job ${scriptId}:`, e.message);
        await supabase.from('story_script').update({ status: 'FAILED', error_message: e.message }).eq('id', scriptId);
    } finally {
        activeJobs.delete(scriptId);
    }
}

const app = express();
app.use(express.json());
app.post(['/render', '/process'], (req, res) => {
    const id = req.body?.id || req.body?.record?.id || req.body?.scriptId || req.body?.payload?.id;
    res.status(202).json({ status: "accepted", id });
    if (id) processJob(id);
});
app.listen(PORT, () => console.log(`Render Engine Online | Port ${PORT}`));
