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
// IMPORTANT: Ensure this matches your actual bucket name for scenes
const BUCKET_SCENES = 'generated-content'; 

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
const activeJobs = new Set();

/**
 * Ensures the URL is a full, valid Supabase public URL.
 */
function ensureFullUrl(input, bucket) {
    if (!input || input === "null" || input === "") return null;
    let str = String(input);
    
    if (str.startsWith('http')) {
        // Fix double URL nesting
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

async function processJob(scriptId) {
    if (!scriptId || activeJobs.has(scriptId)) return;
    activeJobs.add(scriptId);

    try {
        let job = null;
        let retries = 0;
        
        // Wait for Supabase to finish writing the scene data
        while (retries < 20) {
            const { data } = await supabase.from('story_script').select('*').eq('id', scriptId).single();
            // Check if we have scenes to work with
            if (data && data.script_data) {
                let parsed = typeof data.script_data === 'string' ? JSON.parse(data.script_data) : data.script_data;
                if (parsed.scenes && parsed.scenes.length > 0) {
                    job = data;
                    job.parsed_data = parsed;
                    break;
                }
            }
            console.log(`[WAIT] Waiting for scene data for ID ${scriptId}... (${retries}/20)`);
            await new Promise(r => setTimeout(r, 3000));
            retries++;
        }

        if (!job) throw new Error("Timed out waiting for script_data or scenes");

        console.log(`[JOB] Starting render for ID: ${scriptId}`);
        await supabase.from('story_script').update({ status: 'PROCESSING_RENDER', progress_percentage: "40" }).eq('id', scriptId);

        const videoPaths = [];
        const sd = job.parsed_data;
        
        // 1. Logo
        const lPath = await downloadAsset(ensureFullUrl(sd?.logo_video, BUCKET_LOGOS), scriptId, 'logo');
        if (lPath) videoPaths.push(lPath);

        // 2. Scenes
        if (sd?.scenes && Array.isArray(sd.scenes)) {
            for (let i = 0; i < sd.scenes.length; i++) {
                const sUrl = ensureFullUrl(sd.scenes[i].video_url, BUCKET_SCENES);
                const sPath = await downloadAsset(sUrl, scriptId, `scene_${i}`);
                if (sPath) videoPaths.push(sPath);
            }
        }

        if (videoPaths.length === 0) throw new Error("No video scenes were found to stitch.");

        // 3. Music
        const mUrl = ensureFullUrl(sd?.audio_engine?.moodTrack?.url, BUCKET_MUSIC);
        const mPath = await downloadAsset(mUrl, scriptId, 'music', '.mp3');

        const outPath = path.join('/tmp', `output_${scriptId}.mp4`);
        
        // FFmpeg Logic
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
            args.push('-filter_complex', filter, '-map', '[v_out]', '-map', `${videoPaths.length}:a`, '-c:v', 'libx264', '-preset', 'ultrafast', '-c:a', 'aac', '-shortest', '-y', outPath);
        } else {
            args.push('-filter_complex', filter, '-map', '[v_out]', '-c:v', 'libx264', '-preset', 'ultrafast', '-y', outPath);
        }

        await new Promise((resolve, reject) => {
            const proc = spawn('ffmpeg', args);
            proc.on('close', (code) => code === 0 ? resolve() : reject(new Error(`FFmpeg failed`)));
        });

        const videoBuf = await fs.readFile(outPath);
        const finalFileName = `final_${scriptId}_${Date.now()}.mp4`;
        await supabase.storage.from(BUCKET_GENERATED).upload(finalFileName, videoBuf, { contentType: 'video/mp4' });

        const { data: pUrl } = supabase.storage.from(BUCKET_GENERATED).getPublicUrl(finalFileName);
        
        await supabase.from('story_script').update({ 
            status: 'COMPLETED', 
            final_video_url: pUrl.publicUrl, 
            progress_percentage: "100" 
        }).eq('id', scriptId);

        // Cleanup
        for (const p of videoPaths) await fs.unlink(p).catch(() => {});
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

// Support ALL possible incoming ID names from Supabase
app.post(['/render', '/process'], (req, res) => {
    const id = req.body?.id || req.body?.record?.id || req.body?.scriptId || req.body?.payload?.id;
    console.log(`[INCOMING] Request received for ID: ${id}`);
    res.status(202).json({ status: "accepted", id });
    if (id) processJob(id);
});

app.listen(PORT, () => {
    console.log(`Render Engine Online | Port ${PORT}`);
});
