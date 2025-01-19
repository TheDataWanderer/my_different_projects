import os
from pydub import AudioSegment
from pydub.utils import make_chunks
import speech_recognition as sr

audio_file_path = "C:\\Users\\Maks\\Downloads\\Telegram Desktop\\20250118_175306.aac"
wav_file_path = "C:\\Users\\Maks\\Downloads\\Telegram Desktop\\converted_audio.wav"
text_file_path = "C:\\Users\\Maks\\Downloads\\Telegram Desktop\\recognized_text.txt"

# Checking the existence of a file
if not os.path.exists(audio_file_path):
    print(f"The {audio_file_path} file was not found. Check the path.")
    exit()

# Converting AAC to WAV
try:
    print("Converting a file from AAC to WAV...")
    audio = AudioSegment.from_file(audio_file_path, format="aac")
    audio.export(wav_file_path, format="wav")
    print("Conversion complete.")
except Exception as e:
    print(f"Conversion error: {e}")
    exit()

recognizer = sr.Recognizer()

# Loading and splitting audio
try:
    print("Reading and chunking audio...")
    audio = AudioSegment.from_file(wav_file_path, format="wav")
    chunk_length_ms = 150 * 1000  # Slice length in milliseconds (30 seconds)
    chunks = make_chunks(audio, chunk_length_ms)
    print(f"Broken into {len(chunks)} fragments.")
except Exception as e:
    print(f"Error during audio processing: {e}")
    exit()

# Processing of each fragment
recognized_text = ""
for i, chunk in enumerate(chunks):
    try:
        chunk_file = f"chunk_{i}.wav"
        chunk.export(chunk_file, format="wav")  # Saving a temporary fragment
        print(f"A fragment is being processed {i + 1}/{len(chunks)}...")

        with sr.AudioFile(chunk_file) as source:
            audio_data = recognizer.record(source)
            text = recognizer.recognize_google(audio_data, language="ru-RU")
            recognized_text += text + " "
        os.remove(chunk_file)  # Deleting a temporary file
    except sr.UnknownValueError:
        print(f"Fragment {i + 1} not recognized.")
    except sr.RequestError as e:
        print(f"API connection error: {e}")
    except Exception as e:
        print(f"Error during fragment processing {i + 1}: {e}")

# Saving the full text to a file
try:
    with open(text_file_path, "w", encoding="utf-8") as file:
        file.write(recognized_text.strip())
    print(f"Recognized text successfully saved to a file: {text_file_path}")
except Exception as e:
    print(f"Error when saving text: {e}")