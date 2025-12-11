/**
 * @typedef {Object} VideoData
 * @property {number} id
 * @property {Array<Object>} scenes
 * @property {Array<Object>} environments
 * @property {Array<Object>} characters
 * @property {Object} background_music
 * @property {Object} logo_video
 * @property {Object} credits
 */

/**
 * Escapes single quotes and newlines for FFmpeg drawtext filter safety.
 * This function ensures FFmpeg handles text like "Storyloom's" correctly.
 * @param {string} text 
 * @returns {string} The escaped text.
 */
function escapeDrawText(text) {
    if (!text) return '';
    // Replace ' with '\' to escape the quote inside FFmpeg's single-quoted argument, and replace newlines.
    return text.replace(/'/g, "'\\''").replace(/\n/g, '\\n');
}

/**
 * Builds the complete FFmpeg command string by parsing the complex videoData object.
 *
 * @param {VideoData} videoData
 * @returns {string} The complete FFmpeg command.
 */
function buildFFmpegCommand(videoData) {
  const { id, scenes, environments, characters, background_music, logo_video, credits } = videoData;

  let command = 'ffmpeg -y'; // -y forces overwrite without prompt
  const inputs = [];
  const filterComplex = [];
  const sceneSegments = [];

  let inputIndex = 0;
  let logoInputIndex = -1;
  let musicInputIndex = -1;

  // --- Step 1: Add Global Inputs (Logo and Music) ---
  if (logo_video?.url) {
    inputs.push(`-i "${logo_video.url}"`);
    logoInputIndex = inputIndex++;
  }
  if (background_music?.url) {
    inputs.push(`-i "${background_music.url}"`);
    musicInputIndex = inputIndex++;
  }

  // --- Step 2: Process Each Scene ---
  scenes.forEach((scene, sceneIdx) => {
    const sceneDuration = scene.duration || 5;

    const environment = environments.find(e =>
      scene.setting?.toLowerCase().includes(e.name.toLowerCase())
    ) || environments[0]; 

    // Environment Input
    let envInputIndex = -1;
    if (environment?.image_url) {
      inputs.push(`-loop 1 -t ${sceneDuration} -i "${environment.image_url}"`);
      envInputIndex = inputIndex++;
    } else {
      filterComplex.push(`color=c=black:s=1920x1080:d=${sceneDuration}[bg${sceneIdx}]`);
    }

    // Character Inputs (unchanged)
    const sceneCharacterInputs = [];
    for (const charName of scene.characters || []) {
      const character = characters.find(c => c.name === charName);
      if (character?.transparent_image_url) {
        inputs.push(`-loop 1 -t ${sceneDuration} -i "${character.transparent_image_url}"`);
        sceneCharacterInputs.push({
          name: charName,
          inputIndex: inputIndex++,
          character: character
        });
      }
    }

    // Dialogue Audio Inputs (unchanged)
    const dialogueAudioInputs = [];
    let sceneAudioDuration = 0;
    for (const dialogue of scene.dialogue || []) {
      if (dialogue.audio_url) {
        inputs.push(`-i "${dialogue.audio_url}"`);
        dialogueAudioInputs.push({
          character: dialogue.character,
          text: dialogue.text,
          inputIndex: inputIndex++,
          duration: dialogue.duration || 3,
          startTime: sceneAudioDuration
        });
        sceneAudioDuration += dialogue.duration || 3;
      }
    }

    // --- Build Scene Filter Graph ---
    let currentOverlay = (envInputIndex >= 0) ? `bg${sceneIdx}` : `bg${sceneIdx}`;

    if (envInputIndex >= 0) {
      filterComplex.push(
        `[${envInputIndex}:v]scale=1920:1080:force_original_aspect_ratio=increase,crop=1920:1080[bg${sceneIdx}]`
      );
    }

    // Overlay characters (unchanged)
    sceneCharacterInputs.forEach((charInput, charIdx) => {
      const xPos = 300 + (charIdx * 400); 
      const yPos = 400; 
      const overlayName = `scene${sceneIdx}_char${charIdx}_overlay`;

      filterComplex.push(
        `[${charInput.inputIndex}:v]scale=400:-1[char${sceneIdx}_${charIdx}_scaled]`
      );
      filterComplex.push(
        `[${currentOverlay}][char${sceneIdx}_${charIdx}_scaled]overlay=${xPos}:${yPos}[${overlayName}]`
      );
      currentOverlay = overlayName;
    });

    // Add scene text overlay - USING THE NEW ESCAPE FUNCTION
    if (scene.description) {
      const textOverlayName = `scene${sceneIdx}_text_overlay`;
      const escapedText = escapeDrawText(scene.description.substring(0, 100)); // Limit to 100 chars
      filterComplex.push(
        `[${currentOverlay}]drawtext=text='${escapedText}':fontcolor=white:fontsize=24:x=(w-text_w)/2:y=50:box=1:boxcolor=black@0.5:boxborderw=5[${textOverlayName}]`
      );
      currentOverlay = textOverlayName;
    }

    // Trim and set PTS (unchanged)
    filterComplex.push(
      `[${currentOverlay}]trim=duration=${sceneDuration},setpts=PTS-STARTPTS[scene${sceneIdx}_video]`
    );

    // Mix dialogue audio (unchanged)
    if (dialogueAudioInputs.length > 0) {
      const audioMixInputs = dialogueAudioInputs.map(d => `[${d.inputIndex}:a]`).join('');
      filterComplex.push(
        `${audioMixInputs}concat=n=${dialogueAudioInputs.length}:v=0:a=1[scene${sceneIdx}_audio]`
      );
    } else {
      filterComplex.push(
        `anullsrc=channel_layout=stereo:sample_rate=44100:duration=${sceneDuration}[scene${sceneIdx}_audio]`
      );
    }
    
    sceneSegments.push({
      video: `scene${sceneIdx}_video`,
      audio: `scene${sceneIdx}_audio`,
      duration: sceneDuration
    });
  });

  // --- Step 3-8: Final Concatenation, Mixing, and Assembly ---
  
  // Concatenate all scenes (unchanged)
  const concatVideoInputs = sceneSegments.map(s => `[${s.video}]`).join('');
  const concatAudioInputs = sceneSegments.map(s => `[${s.audio}]`).join('');
  filterComplex.push(`${concatVideoInputs}concat=n=${sceneSegments.length}:v=1:a=0[main_video_temp]`);
  filterComplex.push(`${concatAudioInputs}concat=n=${sceneSegments.length}:v=0:a=1[main_audio_temp]`);

  // Add Credits - USING THE NEW ESCAPE FUNCTION
  const creditsDuration = 5;
  const rawCreditsText = `Directed by ${credits.director}\nAnimated by ${credits.animator}`;
  const escapedCreditsText = escapeDrawText(rawCreditsText); // <-- FIX APPLIED HERE

  filterComplex.push(`color=c=black:s=1920x1080:d=${creditsDuration}[credits_bg]`);
  filterComplex.push(`[credits_bg]drawtext=text='${escapedCreditsText}':fontcolor=white:fontsize=48:x=(w-text_w)/2:y=(h-text_h)/2:line_spacing=20[credits_video]`);
  filterComplex.push(`anullsrc=channel_layout=stereo:sample_rate=44100:duration=${creditsDuration}[credits_audio]`);
  filterComplex.push(`[main_video_temp][credits_video]concat=n=2:v=1:a=0[video_with_credits]`);
  filterComplex.push(`[main_audio_temp][credits_audio]concat=n=2:v=0:a=1[audio_with_credits]`);

  // Add Logo (unchanged)
  let finalVideo = 'video_with_credits';
  let finalAudio = 'audio_with_credits';
  if (logoInputIndex >= 0) {
    filterComplex.push(`[${logoInputIndex}:v][video_with_credits]concat=n=2:v=1:a=0[video_final_pre]`);
    filterComplex.push(`[${logoInputIndex}:a][audio_with_credits]concat=n=2:v=0:a=1[audio_final_pre]`);
    finalVideo = 'video_final_pre';
    finalAudio = 'audio_final_pre';
  }

  // Mix Background Music (unchanged)
  if (musicInputIndex >= 0) {
    filterComplex.push(
      `[${finalAudio}][${musicInputIndex}:a]amix=inputs=2:duration=first:dropout_transition=2[audio_final]`
    );
    finalAudio = 'audio_final';
  }
  
  // Assemble Final Command String (unchanged)
  command += ' ' + inputs.join(' ');
  command += ` -filter_complex "${filterComplex.join(';')}"`;
  command += ` -map "[${finalVideo}]" -map "[${finalAudio}]"`;
  command += ` -c:v libx264 -preset medium -crf 23`;
  command += ` -c:a aac -b:a 128k`;
  command += ` -pix_fmt yuv420p`;
  command += ` -movflags +faststart`; 
  command += ` output.mp4`; 

  return command;
}

module.exports = {
  buildFFmpegCommand
};
