const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const { spawn } = require('child_process'); 
const { promises: fs } = require('fs'); 
const path = require('path');
const fetch = require('node-fetch'); 

// Access environment variables provided by Render
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY);

async function runFFmpeg(args) {
    return new Promise((resolve, reject) => {
        const proc = spawn('ffmpeg', args);
        let err = '';
        proc.stderr.on('data', (d) => err += d.toString());
        proc.on('close', (code) => code === 0 ? resolve() : reject(new Error(err)));
    });
}

async function processJob(scriptId) {
    try {
        console.log(`[STITCHER] Starting final assembly for Script: ${scriptId}`);
        
        // 1. Fetch the script data to get the list of finished clips
        const { data: script, error } = await supabase
            .from('story_script')
            .select('*')
            .eq('id', scriptId)
            .single();

        if (error || !script) throw new Error("Script not found");

        const scriptData = typeof script.script_data === 'string' ? JSON.parse(script.script_data) : script.script_data;
        const scenes = scriptData.scenes || [];
        const processedClips = [];

        // 2. Download each finished scene clip
        for (let i = 0; i < scenes.length; i++) {
            const scene = scenes[i];
            const videoUrl = scene.video_url; // This is the clip ScVideo already finished
            
            if (!videoUrl) continue;

            const tempIn = path.join('/tmp', `in_${i}_${scriptId}.mp4`);
            const outTs = path.join('/tmp', `scene_${i}_${scriptId}.ts`);

            console.log(`[STITCHER] Downloading Scene ${i}...`);
            const res = await fetch(videoUrl);
            const buffer = await res.arrayBuffer();
            await fs.writeFile(tempIn, Buffer.from(buffer));

            // Convert to a standard transport stream for a seamless join (no glitches between scenes)
            await runFFmpeg([
                '-i', tempIn,
                '-c:v', 'libx264', '-pix_fmt', 'yuv420p', '-r', '24',
                '-c:a', 'aac', '-ar', '44100',
                '-f', 'mpegts', '-y', outTs
            ]);
            processedClips.push(outTs);
        }

        // 3. Concatenate all scenes into the final Movie
        const finalPath = path.join('/tmp', `final_${scriptId}.mp4`);
        const concatString = `concat:${processedClips.join('|')}`;
        
        console.log(`[STITCHER] Joining ${processedClips.length} scenes...`);
        await runFFmpeg([
            '-i', concatString,
            '-c', 'copy', // Use 'copy' to preserve the lipsync ScVideo already did
            '-bsf:a', 'aac_adtstoasc', 
            '-y', finalPath
        ]);

        // 4. Upload the final Master Movie
        const fileName = `movie_final_${scriptId}_${Date.now()}.mp4`;
        const fileBuffer = await fs.readFile(finalPath);
        
        await supabase.storage.from('generated-content').upload(fileName, fileBuffer, {
            contentType: 'video/mp4',
            upsert: true
        });

        const { data: pub } = supabase.storage.from('generated-content').getPublicUrl(fileName);

        // 5. Update Database
        await supabase.from('story_script').update({
            status: 'COMPLETED',
            final_video_url: pub.publicUrl
        }).eq('id', scriptId);

        console.log(`[SUCCESS] Movie is ready: ${pub.publicUrl}`);

    } catch (e) {
        console.error(`[STITCHER ERROR]`, e);
        await supabase.from('story_script').update({ status: 'FAILED' }).eq('id', scriptId);
    }
}

const app = express().use(express.json());

app.post(['/render', '/process'], (req, res) => {
    const id = req.body.id || req.body.scriptId || (req.body.record && req.body.record.id);
    if (!id) return res.status(400).send("No ID found");
    
    res.sendStatus(202); // Tell Supabase we got the job
    processJob(id);
});

app.listen(PORT);
