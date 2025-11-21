const express = require('express');
const { exec } = require('child_process');
const fs = require('fs');
const path = require('path');

// NOTE: We cannot use external Supabase imports in Node.js easily, 
// so we'll use a simple fetch to update the status.

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

// --- Configuration from Environment Variables ---
// IMPORTANT: These must be set on Railway/Render for the callback to work!
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY; // Use the Service Role Key for privileged updates

// Function to call back to Supabase and update job status
async function updateJobStatus(scriptId, status, outputUrl = null, errorMessage = null) {
    if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
        console.error("Supabase credentials missing. Cannot update status.");
        return;
    }

    const payload = { 
        status: status, 
        final_video_url: outputUrl, 
        error_message: errorMessage 
    };

    try {
        await fetch(`${SUPABASE_URL}/rest/v1/story_scripts?id=eq.${scriptId}`, {
            method: 'PATCH',
            headers: {
                'Content-Type': 'application/json',
                'apikey': SUPABASE_SERVICE_KEY,
                'Authorization': `Bearer ${SUPABASE_SERVICE_KEY}`
            },
            body: JSON.stringify(payload)
        });
        console.log(`Updated script ${scriptId} to status: ${status}`);
    } catch (e) {
        console.error(`Failed to send status update for ${scriptId}:`, e);
    }
}


// --- The Core FFmpeg Rendering Endpoint ---
app.post('/render', async (req, res) => {
    const { 
        command, 
        scriptId, 
        outputBucket 
        // NOTE: The FFmpeg server would need credentials to upload to 'outputBucket'.
        // For this simple version, we'll assume the rendered file is saved locally 
        // and a hypothetical upload service handles the rest.
    } = req.body;

    if (!command || !scriptId) {
        return res.status(400).send({ error: 'Missing command or scriptId.' });
    }

    // 1. Acknowledge and immediately start the background process
    res.status(202).send({ success: true, message: `FFmpeg job started for script ${scriptId}` });

    // 2. Execute the FFmpeg command
    console.log(`Executing FFmpeg command for ${scriptId}: ${command}`);
    
    // NOTE: The command must save the output to a temporary location, e.g., /tmp/output.mp4
    const outputFilePath = path.join('/tmp', `video_${scriptId}.mp4`);
    const finalCommand = command.replace('output.mp4', outputFilePath); // Replace generic output with specific path

    exec(finalCommand, { timeout: 120000 }, async (error, stdout, stderr) => {
        if (error) {
            console.error(`FFmpeg ERROR for ${scriptId}:`, error);
            await updateJobStatus(scriptId, 'RENDERING_FAILED', null, error.message);
            return;
        }

        console.log(`FFmpeg job successful for ${scriptId}. Starting upload...`);
        
        // Hypothetical Upload Logic:
        // In a real system, you would upload outputFilePath to Supabase Storage (outputBucket).
        
        const finalUrl = `https://storage.supabase.com/final_videos/video_${scriptId}.mp4`; // Placeholder URL
        
        // 3. Update the database on success
        await updateJobStatus(scriptId, 'RENDERING_COMPLETE', finalUrl);

        // 4. Clean up the temporary file (important for server health)
        fs.unlink(outputFilePath, (err) => {
            if (err) console.error(`Failed to delete temporary file ${outputFilePath}:`, err);
        });
    });
});

app.get('/', (req, res) => {
    res.send('FFmpeg Renderer is alive and waiting for POST requests on /render');
});

app.listen(PORT, () => {
    console.log(`FFmpeg Renderer listening on port ${PORT}`);
});
