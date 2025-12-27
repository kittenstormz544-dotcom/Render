const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const { spawn } = require('child_process'); 
const { promises: fs } = require('fs'); 
const path = require('path');
const fetch = require('node-fetch'); 

// --- Configuration ---
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY; 
const PORT = process.env.PORT || 10000;

const BUCKET_GENERATED = 'generated-content';
const BUCKET_LOGOS = 'logo-videos';
const BUCKET_MUSIC = 'music-tracks';

const STATUS_PENDING = 'PENDING'; 
const STATUS_IN_PROGRESS = 'PROCESSING_RENDER'; 
const STATUS_COMPLETED = 'RENDERING_COMPLETE'; 
const STATUS_FAILED = 'FAILED'; 

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

/**
 * Fixes the "Double URL" issue where Supabase paths are repeated
 * This handles the nested URL found in your script_data
 */
function cleanUrl(url) {
    if (!url) return null;
    if (typeof url !== 'string') return null;
    
    let targetUrl = url;
    // Fix for the specific error where URL starts with bucket path then full URL
    if (targetUrl.includes('https://') && targetUrl.lastIndexOf('https://') > 0) {
        targetUrl = targetUrl.substring(targetUrl.lastIndexOf('https://'));
    }
    
    // Final check for double encoding or spaces
    return encodeURI(decodeURI(targetUrl));
}

function buildPublicUrl(bucket, fileName) {
    if (!fileName) return null;
    if (fileName.startsWith('http')) return cleanUrl(fileName);
    const cleanName = fileName.replace(/^\/+/, '');
    return encodeURI(`${SUPABASE_URL}/storage/v1/object/public/${bucket}/${cleanName}`);
}

async function downloadAsset(url, scriptId, assetName) {
    if (!url) return null;
    console.log(`[ASSET] Attempting download for ${assetName}: ${url}`);
    try {
        const response = await fetch(url);
        if (response.ok) {
            const ext = assetName === 'music' ? '.mp3' : '.mp4';
            const tempPath = path.join('/tmp', `${assetName}_${scriptId}_${Date.now()}${ext}`);
            const writer = require('fs').createWriteStream(tempPath);
            response.body.pipe(writer);
            return new Promise((resolve) => {
                writer.on('finish', () => resolve(tempPath));
                writer.on('error', (err) => {
                    console.error(`[ASSET] Write error: ${err}`);
                    resolve(null);
                });
            });
        }
        console.error(`[ASSET] ${assetName} HTTP Fail: ${response.status}`);
    } catch (e) {
        console.error(`[ASSET] ${assetName} Exception: ${e.message}`);
    }
    return null;
}

async function processNextJob() {
    let jobs;
    try {
        // Simple select to avoid the "Accepted" (Token A) text parsing error
        const result = await supabase.from('story_script').select('*').eq('status', STATUS_PENDING).limit(1);
        jobs = result.data;
    } catch (err) {
        // Silent catch for polling errors
        return; 
    }

    if (!jobs?.length) return;

    const job = jobs[0];
    const scriptId = job.id;
    
    try {
        // Set to in-progress immediately
        await supabase.from('story_script').update({ 
            status: STATUS_IN_PROGRESS, 
            progress_percentage: "10" 
        }).eq('id', scriptId);

        // 1. Parse Data Safely
        let sd = job.script_data;
        if (typeof sd === 'string') {
            try { sd = JSON.parse(sd); } catch(e) { sd = {}; }
        }
        sd = sd || {};

        console.log(`[JOB] Processing ID: ${scriptId} | Title: ${sd.title || 'Untitled'}`);

        // 2. Resolve Assets
        const { data: logoRow } = await supabase.from('logo_videos')
            .select('video_url')
            .order('created_at', { ascending: false })
            .limit(1).maybeSingle();
        
        const logoUrl = logoRow ? buildPublicUrl(BUCKET_LOGOS, logoRow.video_url) : buildPublicUrl(BUCKET_LOGOS, sd.logo_video);
        
        // Target the specific music path found in your Supabase logs
        const musicUrl = cleanUrl(sd.audio_engine?.moodTrack?.url);
        
        const lPath = await downloadAsset(logoUrl, scriptId, 'logo');
        const mPath = await downloadAsset(musicUrl, scriptId, 'music');

        // 3. FFmpeg Build
        const outPath = path.join('/tmp', `out_${scriptId}_${Date.now()}.mp4`);
        const duration = sd.total_duration || 15;
        
        let inputs = [];
        let filter = "";

        if (lPath) {
            inputs.push('-i', lPath);
            inputs.push('-f', 'lavfi', '-i', `color=c=black:s=1280x720:d=${duration}`);
            filter += "[0:v][1:v]concat=n=2:v=1:a=0[v_out];";
        } else {
            inputs.push('-f', 'lavfi', '-i', `color=c=black:s=1280x720:d=${duration}`);
            filter += "[0:v]copy[v_out];";
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

        const args = [
            ...inputs,
            '-filter_complex', filter,
            '-map', '[v_out]',
            '-map', '[a_out]',
            '-c:v', 'libx264',
            '-pix_fmt', 'yuv420p',
            '-preset', 'ultrafast',
            '-c:a', 'aac',
            '-shortest',
            '-y',
            outPath
        ];

        console.log(`[FFMPEG] Starting render...`);
        await new Promise((resolve, reject) => {
            const proc = spawn('ffmpeg', args);
            proc.on('close', (code) => code === 0 ? resolve() : reject(new Error(`FFmpeg Failed: Code ${code}`)));
        });

        // 4. Upload Result
        const videoBuf = await fs.readFile(outPath);
        const fileName = `renders/${scriptId}_render_${Date.now()}.mp4`;
        await supabase.storage.from(BUCKET_GENERATED).upload(fileName, videoBuf, { contentType: 'video/mp4', upsert: true });

        const { data: pUrl } = supabase.storage.from(BUCKET_GENERATED).getPublicUrl(fileName);
        
        await supabase.from('story_script').update({ 
            status: STATUS_COMPLETED, 
            final_video_url: pUrl.publicUrl,
            progress_percentage: "100" 
        }).eq('id', scriptId);

        console.log(`[JOB] Finished ID: ${scriptId}`);

        // Cleanup
        if (lPath) await fs.unlink(lPath).catch(() => {});
        if (mPath) await fs.unlink(mPath).catch(() => {});
        await fs.unlink(outPath).catch(() => {});

    } catch (e) {
        console.error("Render Job Error:", e);
        await supabase.from('story_script').update({ 
            status: STATUS_FAILED, 
            error_message: e.message 
        }).eq('id', scriptId);
    }
}

const app = express();
app.use(express.json());
app.post(['/render', '/process'], (req, res) => res.sendStatus(202));

app.listen(PORT, () => {
    console.log(`Render Engine active on port ${PORT}`);
    // Check for jobs every 5 seconds
    setInterval(processNextJob, 5000);
});
