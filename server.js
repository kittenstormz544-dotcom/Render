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

const SUPABASE_TABLE_NAME = 'story_script'; 
const SUPABASE_STORAGE_BUCKET = 'generated-content'; 

const POLLING_INTERVAL_MS = 5000; 
const STATUS_PENDING = 'PENDING'; 
const STATUS_IN_PROGRESS = 'PROCESSING_RENDER'; 
const STATUS_COMPLETED = 'RENDERING_COMPLETE'; 
const STATUS_FAILED = 'FAILED'; 

const FALLBACK_LOGO_URL = 'https://voxlcvvksogqktgxyihm.supabase.co/storage/v1/object/public/music-tracks/FallbackLogoVideo.mp4'; 
const FALLBACK_MUSIC_URL = 'https://voxlcvvksogqktgxyihm.supabase.co/storage/v1/object/public/music-tracks/1763757139784-Echoes%20in%20the%20Quiet%20.mp3';

const LOGO_VIDEO_DURATION_SECONDS = 5;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

async function downloadAsset(url, scriptId, assetName) {
    const urlObject = new URL(url);
    let extension = path.extname(urlObject.pathname) || (assetName.includes('music') ? '.mp3' : '.mp4');
    const tempFilePath = path.join('/tmp', `${assetName}_${scriptId}${extension}`);

    console.log(`[ASSET] Downloading ${assetName} from: ${url}`);
    const response = await fetch(url);
    if (!response.ok) throw new Error(`Failed to fetch ${assetName}: ${response.statusText}`);
    
    const writer = require('fs').createWriteStream(tempFilePath);
    response.body.pipe(writer);

    return new Promise((resolve, reject) => {
        writer.on('finish', () => resolve(tempFilePath));
        writer.on('error', reject);
    });
}

async function updateJobStatus(scriptId, status, progress, errorMsg = null, videoUrl = null) {
    const payload = { 
        status, 
        progress_percentage: progress.toString(), 
        error_message: errorMsg, 
        final_video_url: videoUrl 
    };
    await supabase.from(SUPABASE_TABLE_NAME).update(payload).eq('id', scriptId);
    console.log(`Job ${scriptId} -> ${status} (${progress}%)`);
}

async function runFFmpeg(job, logoPath, musicPath) {
    const scriptId = job.id;
    const videoData = job.script_data || {};
    const movieDuration = videoData.total_duration || 20; 
    const totalDuration = movieDuration + LOGO_VIDEO_DURATION_SECONDS;
    const tempOutput = path.join('/tmp', `final_${scriptId}.mp4`);

    const args = [
        '-i', logoPath,
        '-f', 'lavfi', '-i', `color=c=black:s=1280x720:d=${movieDuration}`,
        '-i', musicPath,
        '-filter_complex', 
        `[0:v][1:v]concat=n=2:v=1:a=0[v_full];` + 
        `[2:a]atrim=duration=${totalDuration},afade=t=out:st=${totalDuration - 1}:d=1[a_out]`,
        '-map', '[v_full]', '-map', '[a_out]',
        '-c:v', 'libx264', '-pix_fmt', 'yuv420p', '-c:a', 'aac', '-b:a', '192k', '-y', tempOutput
    ];

    return new Promise((resolve, reject) => {
        const proc = spawn('ffmpeg', args);
        proc.on('close', (code) => code === 0 ? resolve(tempOutput) : reject(new Error(`FFmpeg exited ${code}`)));
    });
}

async function processNextJob() {
    const { data: jobs } = await supabase.from(SUPABASE_TABLE_NAME).select('*').eq('status', STATUS_PENDING).limit(1);
    if (!jobs || jobs.length === 0) return;

    const job = jobs[0];
    try {
        await updateJobStatus(job.id, STATUS_IN_PROGRESS, 10);

        let logoUrl = null;
        if (job.user_id) {
            console.log(`[LOGO] Checking logo_videos for user: ${job.user_id}`);
            const { data: logoData } = await supabase.from('logo_videos').select('url').eq('user_id', job.user_id).maybeSingle();
            if (logoData) logoUrl = logoData.url;
        }

        if (!logoUrl && job.script_data?.logo_video) {
            logoUrl = `${SUPABASE_URL}/storage/v1/object/public/generated-content/${job.script_data.logo_video}`;
        }
        
        logoUrl = logoUrl || FALLBACK_LOGO_URL;
        const musicUrl = job.script_data?.background_music || FALLBACK_MUSIC_URL;

        const lPath = await downloadAsset(logoUrl, job.id, 'logo');
        const mPath = await downloadAsset(musicUrl, job.id, 'music');

        await updateJobStatus(job.id, STATUS_IN_PROGRESS, 40);
        const finalPath = await runFFmpeg(job, lPath, mPath);

        const videoBuf = await fs.readFile(finalPath);
        const storagePath = `public/renders/${job.id}_${Date.now()}.mp4`;
        await supabase.storage.from(SUPABASE_STORAGE_BUCKET).upload(storagePath, videoBuf, { contentType: 'video/mp4', upsert: true });

        const { data: pUrl } = supabase.storage.from(SUPABASE_STORAGE_BUCKET).getPublicUrl(storagePath);
        await updateJobStatus(job.id, STATUS_COMPLETED, 100, null, pUrl.publicUrl);

    } catch (e) {
        console.error("Job Error:", e);
        await updateJobStatus(job.id, STATUS_FAILED, 0, e.message);
    }
}

const app = express();
app.use(express.json());

const handleIncomingJob = async (req, res) => {
    console.log(`Received job request:`, req.body.scriptId || "Polling");
    res.status(202).json({ message: "Job accepted" });
};

app.post('/render', handleIncomingJob);
app.post('/process', handleIncomingJob);

app.listen(PORT, () => {
    console.log(`Server live on port ${PORT}`);
    setInterval(processNextJob, POLLING_INTERVAL_MS);
});
