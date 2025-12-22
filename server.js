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

// Bucket Names confirmed by your App Builder
const BUCKET_GENERATED = 'generated-content';
const BUCKET_LOGOS = 'logo-videos';
const BUCKET_MUSIC = 'music-tracks';

const POLLING_INTERVAL_MS = 5000; 
const STATUS_PENDING = 'PENDING'; 
const STATUS_IN_PROGRESS = 'PROCESSING_RENDER'; 
const STATUS_COMPLETED = 'RENDERING_COMPLETE'; 
const STATUS_FAILED = 'FAILED'; 

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

/**
 * Builds a valid, encoded Public URL for Supabase Storage
 */
function buildPublicUrl(bucket, fileName) {
    if (!fileName) return null;
    if (fileName.startsWith('http')) return encodeURI(fileName);
    // Remove leading slashes and encode the filename (fixes spaces/special chars)
    const cleanName = fileName.replace(/^\/+/, '');
    return encodeURI(`${SUPABASE_URL}/storage/v1/object/public/${bucket}/${cleanName}`);
}

async function downloadAsset(url, scriptId, assetName) {
    if (!url) {
        console.log(`[ASSET] Skipping ${assetName} - No URL provided.`);
        return null;
    }
    
    console.log(`[ASSET] Attempting to download ${assetName}: ${url}`);
    try {
        const response = await fetch(url);
        if (response.ok) {
            const ext = assetName === 'music' ? '.mp3' : '.mp4';
            const tempPath = path.join('/tmp', `${assetName}_${scriptId}_${Date.now()}${ext}`);
            const writer = require('fs').createWriteStream(tempPath);
            response.body.pipe(writer);
            return new Promise((resolve, reject) => {
                writer.on('finish', () => resolve(tempPath));
                writer.on('error', reject);
            });
        }
        throw new Error(`HTTP ${response.status} at ${url}`);
    } catch (e) {
        console.error(`[ASSET] ${assetName} failed: ${e.message}`);
        throw e; 
    }
}

async function processNextJob() {
    const { data: jobs } = await supabase.from('story_script').select('*').eq('status', STATUS_PENDING).limit(1);
    if (!jobs?.length) return;

    const job = jobs[0];
    const scriptId = job.id;
    
    try {
        await supabase.from('story_script').update({ status: STATUS_IN_PROGRESS, progress_percentage: "15" }).eq('id', scriptId);

        // 1. Resolve LOGO using the 'logo-videos' bucket
        const { data: logoRow } = await supabase.from('logo_videos').select('video_url').order('created_at', { ascending: false }).limit(1).maybeSingle();
        const logoUrl = logoRow ? buildPublicUrl(BUCKET_LOGOS, logoRow.video_url) : null;

        // 2. Resolve MUSIC using the 'music-tracks' bucket
        let musicUrl = null;
        let musicRef = job.script_data?.background_music || job.script_data?.music;
        
        if (musicRef) {
            const { data: track } = await supabase.from('music_tracks')
                .select('url, file_path')
                .or(`title.ilike.%${musicRef}%,file_path.ilike.%${musicRef}%`)
                .limit(1).maybeSingle();
            
            if (track) {
                musicUrl = track.url || buildPublicUrl(BUCKET_MUSIC, track.file_path);
            }
        }

        // 3. Download Assets
        const lPath = await downloadAsset(logoUrl, scriptId, 'logo');
        const mPath = await downloadAsset(musicUrl, scriptId, 'music');

        if (!mPath) throw new Error(`Background music reference "${musicRef}" not found in database or download failed.`);

        await supabase.from('story_script').update({ progress_percentage: "40" }).eq('id', scriptId);

        // 4. FFmpeg Processing
        const outPath = path.join('/tmp', `out_${scriptId}.mp4`);
        const duration = job.script_data?.total_duration || 10;

        let args = [];
        if (lPath) {
            args = [
                '-i', lPath,
                '-f', 'lavfi', '-i', `color=c=black:s=1280x720:d=${duration}`,
                '-i', mPath,
                '-filter_complex', '[0:v][1:v]concat=n=2:v=1:a=0[vv]',
                '-map', '[vv]', '-map', '2:a', '-c:v', 'libx264', '-pix_fmt', 'yuv420p', '-c:a', 'aac', '-shortest', '-y', outPath
            ];
        } else {
            args = [
                '-f', 'lavfi', '-i', `color=c=black:s=1280x720:d=${duration}`,
                '-i', mPath,
                '-c:v', 'libx264', '-pix_fmt', 'yuv420p', '-c:a', 'aac', '-map', '0:v', '-map', '1:a', '-shortest', '-y', outPath
            ];
        }

        await new Promise((resolve, reject) => {
            const proc = spawn('ffmpeg', args);
            proc.on('close', (code) => code === 0 ? resolve() : reject(new Error("FFmpeg execution failed")));
        });

        // 5. Upload Render to 'generated-content'
        const videoBuf = await fs.readFile(outPath);
        const fileName = `renders/${scriptId}_${Date.now()}_final.mp4`;
        await supabase.storage.from(BUCKET_GENERATED).upload(fileName, videoBuf, { contentType: 'video/mp4', upsert: true });

        const { data: pUrl } = supabase.storage.from(BUCKET_GENERATED).getPublicUrl(fileName);
        
        await supabase.from('story_script').update({ 
            status: STATUS_COMPLETED, 
            final_video_url: pUrl.publicUrl,
            progress_percentage: "100"
        }).eq('id', scriptId);

        // Cleanup temp files
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
app.post(['/render', '/process'], (req, res) => res.status(202).send({ status: "processing" }));

app.listen(PORT, () => {
    console.log(`Render Engine listening on port ${PORT}`);
    setInterval(processNextJob, POLLING_INTERVAL_MS);
});
