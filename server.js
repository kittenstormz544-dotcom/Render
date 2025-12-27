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

async function processJob(scriptId) {
    try {
        let job = null;
        let retries = 0;
        
        // RETRY LOGIC: Wait for Supabase to actually finish writing the script_data
        while (retries < 15) {
            const { data, error } = await supabase.from('story_script').select('*').eq('id', scriptId).single();
            if (data && data.script_data) {
                job = data;
                break;
            }
            console.log(`[WAIT] script_data for ID ${scriptId} is null. Retrying in 2s... (${retries}/15)`);
            await new Promise(r => setTimeout(r, 2000));
            retries++;
        }

        if (!job || !job.script_data) {
            throw new Error(`The 'script_data' column remained null after 30 seconds of waiting.`);
        }

        console.log(`[JOB] Starting processing for ID: ${scriptId}`);
        await supabase.from('story_script').update({ status: 'PROCESSING_RENDER', progress_percentage: "25" }).eq('id', scriptId);

        let sd = job.script_data;
        if (typeof sd === 'string') { try { sd = JSON.parse(sd); } catch(e) { sd = {}; } }
        
        const videoPaths = [];
        
        // 1. Download Logo (Using optional chaining ?. to prevent crashes)
        const logoUrl = ensureFullUrl(sd?.logo_video, BUCKET_LOGOS);
        const lPath = await downloadAsset(logoUrl, scriptId, 'logo');
        if (lPath) videoPaths.push(lPath);

        // 2. Download Scenes
        if (sd?.scenes && Array.isArray(sd.scenes)) {
            for (let i = 0; i < sd.scenes.length; i++) {
                const scene = sd.scenes[i];
                if (scene.video_url) {
                    const sPath = await downloadAsset(ensureFullUrl(scene.video_url, 'scene-videos'), scriptId, `scene_${i}`);
                    if (sPath) videoPaths.push(sPath);
                }
            }
        }

        // 3. Download Music
        const musicUrl = ensureFullUrl(sd?.audio_engine?.moodTrack?.url, BUCKET_MUSIC);
        const mPath = await downloadAsset(musicUrl, scriptId, 'music');

        // FALLBACK: If absolutely no videos were found, create a simple text placeholder
        if (videoPaths.length === 0) {
            const placeholder = path.join('/tmp', `placeholder_${scriptId}.mp4`);
            await new Promise((res) => {
                const p = spawn('ffmpeg', ['-f', 'lavfi', '-i', `color=c=black:s=1280x720:d=5`, '-vf', `drawtext=text='Generating Story...':fontcolor=white:fontsize=40:x=(w-text_w)/2:y=(h-text_h)/2`, '-pix_fmt', 'yuv420p', '-y', placeholder]);
                p.on('close', res);
            });
            videoPaths.push(placeholder);
        }

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
            args.push('-filter_complex', filter, '-map', '[v_out]', '-map', `${videoPaths.length}:a`, '-c:v', 'libx264', '-preset', 'ultrafast', '-c:a', 'aac', '-shortest', '-y', outPath);
        } else {
            args.push('-filter_complex', filter, '-map', '[v_out]', '-c:v', 'libx264', '-preset', 'ultrafast', '-y', outPath);
        }

        await new Promise((resolve, reject) => {
            const proc = spawn('ffmpeg', args);
            proc.on('close', (code) => code === 0 ? resolve() : reject(new Error(`FFmpeg error ${code}`)));
        });

        const videoBuf = await fs.readFile(outPath);
        const fileName = `render_${scriptId}_${Date.now()}.mp4`;
        await supabase.storage.from(BUCKET_GENERATED).upload(fileName, videoBuf, { contentType: 'video/mp4' });

        const { data: pUrl } = supabase.storage.from(BUCKET_GENERATED).getPublicUrl(fileName);
        await supabase.from('story_script').update({ status: 'COMPLETED', final_video_url: pUrl.publicUrl, progress_percentage: "100" }).eq('id', scriptId);

        console.log(`[SUCCESS] Rendering complete for ID: ${scriptId}`);

        // Cleanup
        for (const p of videoPaths) await fs.unlink(p).catch(() => {});
        if (mPath) await fs.unlink(mPath).catch(() => {});
        await fs.unlink(outPath).catch(() => {});
    } catch (e) {
        console.error(`[FATAL ERROR] ${e.message}`);
        await supabase.from('story_script').update({ status: 'FAILED', error_message: e.message }).eq('id', scriptId);
    }
}

const app = express();
app.use(express.json());

// Listen for POST requests from Supabase
app.post(['/render', '/process'], async (req, res) => {
    const scriptId = req.body?.id || req.body?.record?.id;
    res.status(202).json({ message: "Job received by Render Engine" });
    if (scriptId) processJob(scriptId);
});

app.listen(PORT, () => {
    console.log(`Render Engine active on port ${PORT}`);
    // Periodic check for PENDING jobs
    setInterval(async () => {
        const { data } = await supabase.from('story_script').select('id').eq('status', 'PENDING').limit(1);
        if (data?.length) processJob(data[0].id);
    }, 10000);
});
