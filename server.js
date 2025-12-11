const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const { spawn } = require('child_process'); 
const { promises: fs } = require('fs'); 
const path = require('path');
const fetch = require('node-fetch'); 

// --- Configuration ---
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY; 
const PORT = process.env.PORT || 3000;

const SUPABASE_TABLE_NAME = 'story_script'; 
const SUPABASE_STORAGE_BUCKET = 'generated-content'; 

// CRITICAL CONFIRMED COLUMN NAMES:
const VIDEO_DATA_COLUMN_NAME = 'script_data'; 
const LOGO_VIDEO_URL_COLUMN = 'logo_video_url'; 

const POLLING_INTERVAL_MS = 5000; 

const STATUS_PENDING = 'PENDING'; 
const STATUS_IN_PROGRESS = 'PROCESSING_RENDER'; 
const STATUS_COMPLETED = 'RENDERING_COMPLETE'; 
const STATUS_FAILED = 'FAILED'; 

// Updated Fallback URL to use a generic video placeholder instead of a static image
const FALLBACK_LOGO_URL = 'https://voxlcvvksogqktgxyihm.supabase.co/storage/v1/object/public/music-tracks/FallbackLogoVideo.mp4'; 
const FALLBACK_MUSIC_URL = 'https://voxlcvvksogqktgxyihm.supabase.co/storage/v1/object/public/music-tracks/1763757139784-Echoes%20in%20the%20Quiet%20.mp3';

// TEMPORARY CONSTANT: We assume the logo video is 5 seconds long for concatenation purposes.
// We will replace this with a dynamic ffprobe call in Phase 3.
const LOGO_VIDEO_DURATION_SECONDS = 5;


const supabase = SUPABASE_URL && SUPABASE_SERVICE_KEY 
    ? createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, { auth: { persistSession: false } }) 
    : {};

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
    console.error("CRITICAL ERROR: Supabase credentials missing. App cannot access database.");
}

// =========================================================
// === WORKER UTILITIES (Database and File Handling) =======
// =========================================================

/**
 * Downloads an external asset (video or audio) and saves it to a local temporary file.
 */
async function downloadAsset(url, scriptId, assetName) {
    if (!url) {
        throw new Error(`[ASSET DOWNLOAD] Missing URL for ${assetName}`);
    }
    // Determine file extension from the URL pathname, default to .mp4
    const urlObject = new URL(url);
    // Use .mp4 default for videos, .mp3 for music if extension is missing/generic
    let extension = path.extname(urlObject.pathname);
    if (!extension && assetName.includes('music')) {
        extension = '.mp3';
    } else if (!extension) {
         extension = '.mp4';
    }
    
    const tempFilePath = path.join('/tmp', `${assetName}_${scriptId}${extension}`);

    try {
        console.log(`[ASSET DOWNLOAD] Downloading ${assetName} from: ${url}`);
        const response = await fetch(url);
        if (!response.ok) {
            throw new Error(`Failed to fetch ${assetName}: ${response.statusText}`);
        }
        
        // Use streaming write for large files
        const writer = require('fs').createWriteStream(tempFilePath);
        response.body.pipe(writer);

        return new Promise((resolve, reject) => {
            writer.on('finish', () => {
                console.log(`[ASSET DOWNLOAD] Downloaded ${assetName} successfully to ${tempFilePath}`);
                resolve(tempFilePath);
            });
            writer.on('error', (err) => {
                console.error(`[ASSET DOWNLOAD] Error writing file for ${assetName}: ${err.message}`);
                reject(new Error(`Failed to save downloaded asset (${assetName}): ${err.message}`));
            });
        });
    } catch (e) {
        console.error(`[ASSET DOWNLOAD] Error downloading ${assetName}: ${e.message}`);
        throw new Error(`Failed to download required asset (${assetName}): ${e.message}`);
    }
}


async function uploadVideoToStorage(scriptId, tempFilePath) {
    const storagePath = `public/${scriptId}.mp4`;
    let finalVideoUrl = null;
    try {
        const stats = await fs.stat(tempFilePath);
        if (stats.size === 0) throw new Error("Generated file size is zero. Not uploading.");
        const videoBuffer = await fs.readFile(tempFilePath);
        
        const { error } = await supabase.storage.from(SUPABASE_STORAGE_BUCKET).upload(storagePath, videoBuffer, { contentType: 'video/mp4', upsert: true });
        if (error) throw new Error(`Supabase upload failed: ${error.message}`);

        const { data: publicUrlData } = supabase.storage.from(SUPABASE_STORAGE_BUCKET).getPublicUrl(storagePath);
        finalVideoUrl = publicUrlData?.publicUrl;
        console.log(`[WORKER] SUCCESS: Video uploaded. Public URL: ${finalVideoUrl}`);
    } catch (e) {
        console.error(`[WORKER] UPLOAD ERROR for job ${scriptId}:`, e.message);
        throw e; 
    } finally {
        try { await fs.unlink(tempFilePath); } catch (e) { console.warn(`[WORKER] Clean up warning: File not found at ${tempFilePath}.`); }
    }
    return finalVideoUrl;
}

async function updateJobStatus(scriptId, status, progress_percentage, error_message = null, final_video_url = null) {
    if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) return;
    
    const payload = { status, progress_percentage, error_message, final_video_url };
    const url = `${SUPABASE_URL}/rest/v1/${SUPABASE_TABLE_NAME}?id=eq.${scriptId}`;

    try {
        const response = await fetch(url, {
            method: 'PATCH',
            headers: {
                'Content-Type': 'application/json',
                'apikey': SUPABASE_SERVICE_KEY,
                'Authorization': `Bearer ${SUPABASE_SERVICE_KEY}`,
                'Prefer': 'return=minimal' 
            },
            body: JSON.stringify(payload)
        });
        if (!response.ok) {
            console.error(`Supabase UPDATE failed: ${response.status}`, await response.text());
            return false;
        }
        console.log(`Supabase UPDATE successful. Job ${scriptId} is now ${status}.`);
        return true;
    } catch (e) {
        console.error(`Failed to update job status:`, e);
        return false;
    }
}

// =================================================================
// === FFmpeg EXECUTION (Real Command) =============================
// =================================================================

function executeFFmpeg(args, scriptId, tempFilePath) {
    return new Promise((resolve, reject) => {
        console.log(`[FFMPEG] Starting real FFmpeg execution for job ${scriptId}.`);
        console.log(`[FFMPEG] COMMAND: ffmpeg ${args.join(' ')}`);

        // Spawn the FFmpeg process
        const ffmpeg = spawn('ffmpeg', args);

        ffmpeg.stderr.on('data', (data) => {
            const output = data.toString();
            // Log non-progress related errors/warnings
            if (output.includes('error') || output.includes('failed') || output.includes('invalid')) {
                console.error(`[FFMPEG ERR] ${output.trim()}`);
            } else if (output.includes('time=')) {
                // Ignore time progress updates for cleaner logs
            } else {
                 // Log other output if needed for debugging the command itself
                // console.log(`[FFMPEG OUT] ${output.trim()}`);
            }
        });

        ffmpeg.on('close', (code) => {
            if (code === 0) {
                console.log(`[FFMPEG] FFmpeg Job ${scriptId} finished successfully with code ${code}.`);
                resolve({ success: true });
            } else {
                const errorMessage = `FFmpeg server error: Process exited with code ${code}`;
                console.error(`[FFMPEG] FAILURE: ${errorMessage}`);
                reject(new Error(errorMessage));
            }
        });

        ffmpeg.on('error', (err) => {
            const errorMessage = `FFmpeg server error: Failed to start process: ${err.message}`;
            console.error(`[FFMPEG] CRITICAL FAILURE: ${errorMessage}`);
            reject(new Error(errorMessage));
        });
    });
}
// ------------------------------------------------------------------

/**
 * PHASE 2: Build command to concatenate the Logo Video (Input 0) and the Black Canvas (Input 1),
 * while trimming and fading the Background Music (Input 2).
 * @param {object} job - The job data from Supabase.
 * @param {string} scriptId - The job ID.
 * @param {string} logoVideoPath - Local path to the downloaded logo video.
 * @param {string} musicPath - Local path to the downloaded music.
 * @returns {{args: string[], tempFilePath: string}}
 */
function buildFFmpegCommand(job, scriptId, logoVideoPath, musicPath) {
    const videoData = job[VIDEO_DATA_COLUMN_NAME] || {};
    
    const DEFAULT_DURATION = 20; 
    // Total duration of the final video (Logo + Main Content)
    const totalDuration = videoData.total_duration && !isNaN(videoData.total_duration) && videoData.total_duration > 0 ? videoData.total_duration : DEFAULT_DURATION; 
    
    // Duration of the Main Content Canvas (Total Duration minus Logo duration)
    const canvasDuration = Math.max(0, totalDuration - LOGO_VIDEO_DURATION_SECONDS);

    const outputFileName = `output_${scriptId}.mp4`;
    const tempFilePath = path.join('/tmp', outputFileName);
    
    console.log(`[WORKER] Building FFmpeg Command for Job ${scriptId}. Total Duration: ${totalDuration}s. Canvas Duration: ${canvasDuration}s.`);
    
    // FFmpeg arguments
    const args = [
        // Input 0: Logo Video (The Intro)
        '-i', logoVideoPath, 
        
        // Input 1: Black background video (The Main Content Canvas)
        '-f', 'lavfi', '-i', `color=c=black:s=1280x720:d=${canvasDuration}`, 
        
        // Input 2: Background Music (Assumed long track)
        '-i', musicPath, 

        '-filter_complex', 
        // 1. Concatenate video inputs: Logo (0:v) and Black Canvas (1:v)
        `[0:v][1:v]concat=n=2:v=1:a=0[v_full];` + 
        // 2. Trim and fade out audio (Input 2:a) to match total duration
        // atrim: cuts the audio to the exact totalDuration
        // afade: starts a 1 second fade-out (d=1) at the point (totalDuration - 1)
        `[2:a]atrim=duration=${totalDuration},afade=t=out:st=${totalDuration - 1}:d=1[a_out]`,

        // Map the final video stream [v_full]
        '-map', '[v_full]',
        
        // Map the faded audio stream [a_out]
        '-map', '[a_out]', 
        
        // Audio/Video encoding parameters
        '-c:v', 'libx264', 
        '-pix_fmt', 'yuv420p', 
        '-c:a', 'aac', 
        '-b:a', '192k',
        
        '-y', // Overwrite output file if it exists
        tempFilePath 
    ];

    return { args, tempFilePath };
}

// =========================================================
// === MAIN WORKER LOOP (Polling for PENDING jobs) =========
// =========================================================

let isProcessingJob = false;

async function processJob(job) {
    if (isProcessingJob) return; 
    isProcessingJob = true;
    
    const scriptId = job.id;
    const userId = job.user_id; // Retrieve user_id for logo lookup
    let tempFilePath = '';
    let logoVideoPath = '';
    let musicPath = '';
    
    try {
        const videoData = job[VIDEO_DATA_COLUMN_NAME] || {}; 
        const duration = videoData.total_duration || 20; 
        
        let logoVideoUrl = job[LOGO_VIDEO_URL_COLUMN];

        // --- NEW LOGIC: Dynamic Logo Fetch from 'logo_videos' if URL is missing ---
        if (!logoVideoUrl && userId) {
            console.log(`[WORKER] logo_video_url is missing. Attempting to fetch default logo for user ${userId} from 'logo_videos' table.`);
            const { data: logoData, error: logoError } = await supabase
                .from('logo_videos')
                .select('url')
                .eq('user_id', userId)
                .limit(1)
                .single();
                
            if (logoData) {
                logoVideoUrl = logoData.url;
                console.log(`[WORKER] Retrieved User Logo URL: ${logoVideoUrl}`);
            } else if (logoError && logoError.code !== 'PGRST116') { // PGRST116 is "No rows found"
                console.warn(`[WORKER] Error fetching logo for user ${userId}: ${logoError.message}`);
            }
        }
        
        // Fallback if still no URL found
        logoVideoUrl = logoVideoUrl || FALLBACK_LOGO_URL;
        console.log(`[WORKER] Final Logo URL to use: ${logoVideoUrl}`);
        // --------------------------------------------------------------------------

        const musicUrl = videoData.background_music || FALLBACK_MUSIC_URL;


        console.log(`[WORKER] Starting Phase 2: Intro Concat & Audio Fade for job ${scriptId}. Total Duration: ${duration}s`);
        
        await updateJobStatus(scriptId, STATUS_IN_PROGRESS, 5);
        
        // 1. Download Assets
        logoVideoPath = await downloadAsset(logoVideoUrl, scriptId, 'logo_video');
        musicPath = await downloadAsset(musicUrl, scriptId, 'background_music');
        
        await updateJobStatus(scriptId, STATUS_IN_PROGRESS, 15);

        // 2. Build FFmpeg Command
        const commandData = buildFFmpegCommand(job, scriptId, logoVideoPath, musicPath); 
        tempFilePath = commandData.tempFilePath;

        await updateJobStatus(scriptId, STATUS_IN_PROGRESS, 25);
        
        // 3. Execute FFmpeg (Real Command)
        await executeFFmpeg(commandData.args, scriptId, tempFilePath); 
        
        await updateJobStatus(scriptId, STATUS_IN_PROGRESS, 75);

        // 4. Upload result
        const finalVideoUrl = await uploadVideoToStorage(scriptId, tempFilePath);
        
        // 5. Final status update
        await updateJobStatus(scriptId, STATUS_COMPLETED, 100, null, finalVideoUrl);

    } catch (error) {
        console.error(`[WORKER] Job ${scriptId} failed:`, error);
        // Ensure the error message is clean for the database
        const cleanErrorMessage = error.message.startsWith('FFmpeg server error:') ? error.message : `Worker failed: ${error.message}`;
        await updateJobStatus(scriptId, STATUS_FAILED, 0, cleanErrorMessage);
    } finally {
        isProcessingJob = false;
        
        // Clean up temporary files
        if (logoVideoPath) { try { await fs.unlink(logoVideoPath); } catch (e) { console.warn(`[CLEANUP] Failed to delete logo: ${e.message}`); } }
        if (musicPath) { try { await fs.unlink(musicPath); } catch (e) { console.warn(`[CLEANUP] Failed to delete music: ${e.message}`); } }
        // tempFilePath is cleaned up by uploadVideoToStorage if successful, otherwise it's left for next worker run to clean up
    }
}

async function fetchAndProcessJobs() {
    if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) return; 
    if (isProcessingJob) return;
    
    console.log(`[WORKER CORE] Polling... Phase 2 (Intro Concat & Audio Fade) active. Waiting for PENDING jobs.`);

    // Fetch the single oldest PENDING job, including user_id to look up the logo
    const { data: jobs, error } = await supabase
        .from(SUPABASE_TABLE_NAME)
        .select(`id, ${VIDEO_DATA_COLUMN_NAME}, ${LOGO_VIDEO_URL_COLUMN}, user_id, series_id`) 
        .eq('status', STATUS_PENDING) 
        .limit(1);

    if (error) {
        console.error('[WORKER] Error fetching jobs:', error.message);
        return;
    }

    if (jobs && jobs.length > 0) {
        console.log(`[WORKER] Found PENDING job ${jobs[0].id}. Initiating process.`);
        await processJob(jobs[0]); 
    }
}


// =========================================================
// === EXPRESS WEB SERVICE (API endpoints) =================
// =========================================================

const app = express();
app.use(express.json());

// --- /RENDER ENDPOINT (Queueing new jobs) ---
app.post('/render', async (req, res) => {
    if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
        return res.status(503).send({ error: 'Server misconfigured. Missing Supabase credentials.' });
    }

    const { videoData, scriptId, logoVideoUrl, userId, seriesId } = req.body; 
    
    if (!scriptId) return res.status(400).send({ error: 'Missing scriptId.' });

    const fullScriptText = videoData?.scenes
        ? videoData.scenes.map(scene => scene.description).join('\n---\n')
        : "Script data missing upon queueing.";
    
    const payload = { 
        id: scriptId,
        status: STATUS_PENDING, 
        progress_percentage: 0.0,
        title: videoData?.title || "Untitled Video",
        full_script: fullScriptText, 
        environment_tag: videoData?.animation_style || "2D", 
        content_type: videoData?.content_type || "cartoon", 
        main_character_names: videoData?.script_analysis?.mainCharacters || [],
        
        [LOGO_VIDEO_URL_COLUMN]: logoVideoUrl || null, // Allow this to be null, worker will fetch it.
        user_id: userId || null, 
        series_id: seriesId || null, 

        [VIDEO_DATA_COLUMN_NAME]: videoData || {}
    };
    
    const url = `${SUPABASE_URL}/rest/v1/${SUPABASE_TABLE_NAME}`;

    try {
        const response = await fetch(url, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'apikey': SUPABASE_SERVICE_KEY,
                'Authorization': `Bearer ${SUPABASE_SERVICE_KEY}`,
                'Prefer': 'resolution=merge-duplicates'
            },
            body: JSON.stringify(payload)
        });
        
        if (!response.ok) {
            const errorText = await response.text();
            console.error(`Supabase queue insert failed: ${response.status}`, errorText);
            return res.status(500).send({ error: 'Failed to queue job', details: errorText });
        }
        
        console.log(`Job ${scriptId} queued instantly. Background loop will process.`);
        res.status(202).send({ 
            success: true, 
            message: `FFmpeg job queued successfully for script ${scriptId}. Background processing started.` 
        });
    } catch (error) {
        console.error('Queue error:', error);
        res.status(500).send({ error: 'Internal server error' });
    }
});

// --- Health Check / Root Endpoint ---
app.get('/', (req, res) => res.send('Storyloom Dual-Purpose Web Service Ready.'));

// --- Start the server ---
app.listen(PORT, () => {
    console.log(`Web Service listening on port ${PORT}`);
    
    // --- START THE BACKGROUND POLLING LOOP ---
    if (SUPABASE_URL && SUPABASE_SERVICE_KEY) {
        setInterval(fetchAndProcessJobs, POLLING_INTERVAL_MS);
        console.log(`Background worker loop initialized. Checking for jobs every ${POLLING_INTERVAL_MS / 1000}s.`);
    } else {
        console.warn('Background worker disabled due to missing Supabase configuration.');
    }
});