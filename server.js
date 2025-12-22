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

const LOGO_VIDEO_DURATION_SECONDS = 5;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

/**
 * Encodes a URL safely and builds the path if only a filename is provided
 */
function getSafeUrl(input) {
    if (!input) return null;
    let fullUrl = input;
    if (!input.startsWith('http')) {
        // Default to generated-content bucket if just a filename
        fullUrl = `${SUPABASE_URL}/storage/v1/object/public/${SUPABASE_STORAGE_BUCKET}/${input}`;
    }
    return encodeURI(fullUrl);
}

async function downloadAsset(url, scriptId, assetName) {
    const safeUrl = getSafeUrl(url);
    if (!safeUrl) return null;
    
    console.log(`[ASSET] Downloading ${assetName} from: ${safeUrl}`);
    const response = await fetch(safeUrl);
    
    if (!response.ok) {
        console.error(`[ASSET] Failed to fetch ${assetName}: ${response.status} ${response.statusText}`);
        return null;
    }
    
    const extension = assetName.includes('music') ? '.mp3' : '.mp4';
    const tempFilePath = path.join('/tmp', `${assetName}_${scriptId}_${Date.now()}${extension}`);
    
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
        error_message: errorMsg ? errorMsg.substring(0, 500) : null, 
        final_video_url: videoUrl 
    };
    await supabase.from(SUPABASE_TABLE_NAME).update(payload).eq('id', scriptId);
    console.log(`Job ${scriptId} -> ${status} (${progress}%)`);
}

async function runFFmpeg(job, logoPath, musicPath) {
    const scriptId = job.id;
    const movieDuration = job.script_data?.total_duration || 20; 
    const tempOutput = path.join('/tmp', `final_${scriptId}_${Date.now()}.mp4`);

    let args = [];
    if (logoPath) {
        const totalDuration = movieDuration + LOGO_VIDEO_DURATION_SECONDS;
        args = [
            '-i', logoPath,
            '-f', 'lavfi', '-i', `color=c=black:s=1280x720:d=${movieDuration}`,
            '-i', musicPath,
            '-filter_complex', 
            `[0:v][1:v]concat=n=2:v=1:a=0[v_full];` + 
            `[2:a]atrim=duration=${totalDuration},afade=t=out:st=${totalDuration - 1}:d=1[a_out]`,
            '-map', '[v_full]', '-map', '[a_out]',
            '-c:v', 'libx264', '-pix_fmt', 'yuv420p', '-c:a', 'aac', '-b:a', '192k', '-y', tempOutput
        ];
    } else {
        args = [
            '-f', 'lavfi', '-i', `color=c=black:s=1280x720:d=${movieDuration}`,
            '-i', musicPath,
            '-filter_complex', 
            `[1:a]atrim=duration=${movieDuration},afade=t=out:st=${movieDuration - 1}:d=1[a_out]`,
            '-map', '0:v', '-map', '[a_out]',
            '-c:v', 'libx264', '-pix_fmt', 'yuv420p', '-c:a', 'aac', '-b:a', '192k', '-y', tempOutput
        ];
    }

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

        // --- 1. RESOLVE LOGO ---
        let logoUrl = null;
        if (job.user_id) {
            const { data } = await supabase.from('logo_videos').select('video_url').eq('user_id', job.user_id).maybeSingle();
            if (data) logoUrl = data.video_url;
        }
        if (!logoUrl) {
            const { data } = await supabase.from('logo_videos').select('video_url').order('created_at', { ascending: false }).limit(1).maybeSingle();
            if (data) logoUrl = data.video_url;
        }

        // --- 2. RESOLVE MUSIC (Search music_tracks table) ---
        let musicUrl = job.script_data?.background_music;
        
        // If the background_music field is just an ID or name, look it up in the music_tracks table
        if (musicUrl && !musicUrl.startsWith('http')) {
            console.log(`[MUSIC] Looking up track details for: ${musicUrl}`);
            const { data: trackData } = await supabase
                .from('music_tracks')
                .select('file_path, url')
                .or(`title.eq."${musicUrl}",file_path.eq."${musicUrl}"`)
                .maybeSingle();
            
            if (trackData) {
                musicUrl = trackData.url || trackData.file_path;
                // If it's a file path, we might need to build the bucket URL
                if (musicUrl && !musicUrl.startsWith('http')) {
                    musicUrl = `${SUPABASE_URL}/storage/v1/object/public/music-tracks/${musicUrl}`;
                }
            }
        }

        // --- 3. DOWNLOAD ---
        const lPath = await downloadAsset(logoUrl, job.id, 'logo');
        const mPath = await downloadAsset(musicUrl, job.id, 'music');

        if (!mPath) throw new Error("Could not download background music from music_tracks table.");

        await updateJobStatus(job.id, STATUS_IN_PROGRESS, 50);
        
        const finalPath = await runFFmpeg(job, lPath, mPath);

        const videoBuf = await fs.readFile(finalPath);
        const storagePath = `public/renders/final_${job.id}_${Date.now()}.mp4`;
        await supabase.storage.from(SUPABASE_STORAGE_BUCKET).upload(storagePath, videoBuf, { contentType: 'video/mp4', upsert: true });

        const { data: pUrl } = supabase.storage.from(SUPABASE_STORAGE_BUCKET).getPublicUrl(storagePath);
        await updateJobStatus(job.id, STATUS_COMPLETED, 100, null, pUrl.publicUrl);

        // Clean up
        if (lPath) await fs.unlink(lPath).catch(() => {});
        await fs.unlink(mPath).catch(() => {});
        await fs.unlink(finalPath).catch(() => {});

    } catch (e) {
        console.error("Job Error:", e);
        await updateJobStatus(job.id, STATUS_FAILED, 0, e.message);
    }
}

const app = express();
app.use(express.json());
app.post(['/render', '/process'], (req, res) => res.status(202).send({ status: "accepted" }));

app.listen(PORT, () => {
    console.log(`Server live on port ${PORT}`);
    setInterval(processNextJob, POLLING_INTERVAL_MS);
});
