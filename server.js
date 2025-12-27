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
const activeJobs = new Set();

/**
 * Ensures the URL is a full, valid Supabase public URL.
 * Also handles the "Clean URL" logic your AI implemented.
 */
function ensureFullUrl(input, bucket) {
    if (!input || input === "null" || input === "") return null;
    let str = String(input);
    
    // If it's already a full URL, just return it
    if (str.startsWith('http')) {
        // Fix double URL issues if they still occur
        if (str.includes('https://') && str.lastIndexOf('https://') > 0) {
            str = str.substring(str.lastIndexOf('https://'));
        }
        return encodeURI(decodeURI(str));
    }
    
    // Otherwise, build the path from the bucket
    return `${SUPABASE_URL}/storage/v1/object/public/${bucket}/${str.replace(/^\/+/, '')}`;
}

/**
 * Downloads a video or audio file to the /tmp directory.
 */
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
        } else {
            console.log(`[DOWNLOAD] Failed (${response.status}): ${assetName} at ${url}`);
        }
    } catch (e) { console.error(`[DOWNLOAD ERROR] ${assetName}: ${e.message}`); }
    return null;
}

/**
 * Main rendering logic
 */
async function processJob(scriptId) {
    if (activeJobs.has(scriptId)) return;
    activeJobs.add(scriptId);

    try {
        let job = null;
        let retries = 0;
        
        // Retry loop to wait for Supabase to finish writing data
        while (retries < 15) {
            const { data } = await supabase.from('story_script').select('*').eq('id', scriptId).single();
            if (data && data.script_data) {
                job = data;
                break;
            }
            console.log(`[WAIT] script_data for ID ${scriptId} is null. Retrying... (${retries}/15)`);
            await new Promise(r => setTimeout(r, 2000));
            retries++;
        }

        if (!job || !job.script_data) throw new Error("Could not find script_data after 30s");

        console.log(`[JOB] Starting render for ID: ${scriptId}`);
        await supabase.from('story_script').update({ status: 'PROCESSING_RENDER', progress_percentage: "40" }).eq('id', scriptId);

        let sd = job.script_data;
        if (typeof sd === 'string') { try { sd = JSON.parse(sd); } catch(e) { sd = {}; } }
        
        const videoPaths = [];
        
        // 1. Download Logo
        const lPath = await downloadAsset(ensureFullUrl(sd?.logo_video, BUCKET_LOGOS), scriptId, 'logo');
        if (lPath) videoPaths.push(lPath);

        // 2. Download Scenes
        if (sd?.scenes && Array.isArray(sd.scenes)) {
            for (let i = 0; i < sd.scenes.length; i++) {
                const sUrl = ensureFullUrl(sd.scenes[i].video_url, 'scene-videos');
                const sPath = await downloadAsset(sUrl, scriptId, `scene_${i}`);
                if (sPath) videoPaths.push(sPath);
            }
        }

        // 3. Download Music (Use .mp3 or .wav depending on track)
        const rawMusicUrl = sd?.audio_engine?.moodTrack?.url;
        const mPath = await downloadAsset(ensureFullUrl(rawMusicUrl, BUCKET_MUSIC), scriptId, 'music', '.mp3');

        if (videoPaths.length === 0) throw new Error("No video scenes were found to stitch.");

        const outPath = path.join('/tmp', `output_${scriptId}.mp4`);
        
        // Build FFmpeg Filter Complex
        let filter = "";
        let inputArgs = [];
        videoPaths.forEach((p, idx) => {
            inputArgs.push('-i', p);
            // Standardize all clips to 1280x720, 25fps, yuv420p
            filter += `[${idx}:v]scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2,setsar=1,fps=25,format=yuv420p[v${idx}];`;
        });
        
        const videoNodes = videoPaths.map((_, i) => `[v${i}]`).join('');
        filter += `${videoNodes}concat=n=${videoPaths.length}:v=1:a=0[v_out]`;

        let args = [...inputArgs];
        if (mPath) {
            args.push('-i', mPath);
            args.push('-filter_complex', filter, '-map', '[v_out]', '-map', `${videoPaths.length}:a`, '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '23', '-c:a', 'aac', '-b:a', '128k', '-shortest', '-y', outPath);
        } else {
            args.push('-filter_complex', filter, '-map', '[v_out]', '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '23', '-y', outPath);
        }

        // Execute FFmpeg
        await new Promise((resolve, reject) => {
            const proc = spawn('ffmpeg', args);
            proc.on('close', (code) => code === 0 ? resolve() : reject(new Error(`FFmpeg failed with code ${code}`)));
        });

        // Upload Final Video
        const videoBuf = await fs.readFile(outPath);
        const finalFileName = `final_${scriptId}_${Date.now()}.mp4`;
        const { error: uploadError } = await supabase.storage.from(BUCKET_GENERATED).upload(finalFileName, videoBuf, { contentType: 'video/mp4' });
        
        if (uploadError) throw uploadError;

        const { data: pUrl } = supabase.storage.from(BUCKET_GENERATED).getPublicUrl(finalFileName);
        
        // Mark as Completed
        await supabase.from('story_script').update({ 
            status: 'COMPLETED', 
            final_video_url: pUrl.publicUrl, 
            progress_percentage: "100" 
        }).eq('id', scriptId);

        console.log(`[SUCCESS] Generated movie for ID: ${scriptId}`);

        // Cleanup temp files
        for (const p of videoPaths) await fs.unlink(p).catch(() => {});
        if (mPath) await fs.unlink(mPath).catch(() => {});
        await fs.unlink(outPath).catch(() => {});

    } catch (e) {
        console.error(`[FATAL] Job ${scriptId} failed:`, e.message);
        await supabase.from('story_script').update({ status: 'FAILED', error_message: e.message }).eq('id', scriptId);
    } finally {
        activeJobs.delete(scriptId);
    }
}

const app = express();
app.use(express.json());

// Endpoint for Supabase to hit
app.post(['/render', '/process'], (req, res) => {
    const id = req.body?.id || req.body?.record?.id || req.body?.scriptId;
    res.status(202).json({ status: "accepted", id });
    if (id) processJob(id);
});

app.listen(PORT, () => {
    console.log(`Render Engine Online | Port ${PORT}`);
    
    // Safety Poller: Check for stuck PENDING jobs every 15 seconds
    setInterval(async () => {
        const { data } = await supabase.from('story_script')
            .select('id')
            .eq('status', 'PENDING')
            .limit(1);
        if (data?.length) processJob(data[0].id);
    }, 15000);
});
