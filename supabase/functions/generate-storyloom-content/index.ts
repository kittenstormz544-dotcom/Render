// supabase/functions/generate-storyloom-content/index.ts
import { serve } from 'https://deno.land/std@0.177.0/http/server.ts';
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2.44.0';

// Import helpers for cinematic effects and rendering
import { generateEnvironmentPrompt, determineCameraShot } from './helpers/visualDirector.ts';
import { generateFFmpegCommand } from './helpers/ffmpegComposer.ts'; 

// --- Configuration ---
const HUGGINGFACE_API_KEY = Deno.env.get('HUGGINGFACE_API_KEY');
const FFMPEG_SERVER_URL = Deno.env.get('FFMPEG_SERVER_URL');
// New requirement: The server needs to call back, so we pass the public Supabase URL.
const SUPABASE_URL = Deno.env.get('SUPABASE_URL');
// The FFmpeg Server needs a special key to update the DB status (Service Role Key).
const SUPABASE_SERVICE_KEY = Deno.env.get('SUPABASE_SERVICE_KEY');

if (!HUGGINGFACE_API_KEY || !FFMPEG_SERVER_URL || !SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
    throw new Error('Missing environment variables: HUGGINGFACE_API_KEY, FFMPEG_SERVER_URL, SUPABASE_URL, and SUPABASE_SERVICE_KEY are required.');
}

serve(async (req) => {
    const supabaseClient = createClient(
        SUPABASE_URL ?? '',
        Deno.env.get('SUPABASE_ANON_KEY') ?? '',
        { global: { headers: { 'Authorization': req.headers.get('Authorization')! } } }
    );

    // 1. Get Job Details (The trigger)
    const { record: scriptRecord } = await req.json();
    const scriptId = scriptRecord.id;

    // Set initial status: PRE-PROCESSING
    await supabaseClient.from('story_scripts').update({ status: 'PRE-PROCESSING' }).eq('id', scriptId);

    try {
        // --- 2. ASSET GATHERING AND AI CALLS (Same as before) ---
        
        // ... (Parsing logic and AI calls happen here)
        
        const parsedScenes = [
            {
                location: scriptRecord.environment_tag,
                dialogue: "Okay, this is definitely not on the tour map...",
                character: 'Emma',
                action: 'steps cautiously through the underbrush.',
                emotion: 'cautious', 
                duration_seconds: 7, 
                style: 'anime', 
            }
        ];

        let finalSceneCommands: any[] = [];
        let totalVideoDuration = 0;

        // --- (Loop logic to gather assets and duration remains the same) ---
        for (const scene of parsedScenes) {
            const { data: characterData } = await supabaseClient.from('character_assets').select('transparent_image_url, voice_preset_id').eq('character_name', scene.character).single();
            const { data: envData } = await supabaseClient.from('environment_assets').select('dynamic_prompt, ambient_audio_url').eq('name', scene.location).single();
            
            // NOTE: Hugging Face API calls would go here to get audio/video URLs.
            const dialogueAudioUrl = "https://storage.supabase.com/v1/storage/clips/dialogue_1.wav"; // Placeholder 
            const backgroundVideoUrl = `https://storage.supabase.com/v1/storage/clips/video_${scriptId}_${finalSceneCommands.length}.mp4`; // Placeholder

            const cameraShot = determineCameraShot(scene.action, scene.duration_seconds);
            
            finalSceneCommands.push({
                video_url: backgroundVideoUrl,
                audio_url: dialogueAudioUrl,
                ambient_url: envData!.ambient_audio_url,
                character_png_url: characterData!.transparent_image_url,
                duration_seconds: scene.duration_seconds,
                camera_zoom_pan_filter: cameraShot
            });
            totalVideoDuration += scene.duration_seconds;
        }


        // --- 3. KICK OFF FFmpeg RENDERING (Pass callback info) ---
        
        const ffmpegCommand = generateFFmpegCommand(finalSceneCommands, scriptRecord.logo_video_url, totalVideoDuration);

        // Update status before sending to server
        await supabaseClient.from('story_scripts').update({ status: 'QUEUED_FOR_RENDER' }).eq('id', scriptId);

        // Send job to the FFmpeg Server
        const renderJobResponse = await fetch(FFMPEG_SERVER_URL, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                command: ffmpegCommand,
                scriptId: scriptId,
                outputBucket: 'storyloom-videos', // Used for final upload
                // The FFmpeg server will read these environment variables to call back.
                // We don't need to pass them in the body, just ensure they are set as secrets
                // on the Railway/Render deployment.
            })
        });

        if (!renderJobResponse.ok) {
            throw new Error(`FFmpeg Server failed to accept job: ${renderJobResponse.statusText}. Check FFMPEG_SERVER_URL.`);
        }

        // 4. Success message (The FFmpeg server handles the final 'RENDERING_COMPLETE' status)
        return new Response(JSON.stringify({ success: true, message: 'Rendering job successfully sent to FFmpeg server. Status will update shortly.' }), {
            headers: { 'Content-Type': 'application/json' },
            status: 200,
        });

    } catch (error) {
        console.error('Processing Error:', error);
        // Set final status to FAILED if orchestration fails
        await supabaseClient.from('story_scripts').update({ status: 'FAILED', error_message: (error as Error).message }).eq('id', scriptId);

        return new Response(JSON.stringify({ error: (error as Error).message }), {
            headers: { 'Content-Type': 'application/json' },
            status: 500,
        });
    }
});
