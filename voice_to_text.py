import os
import subprocess
import speech_recognition as sr
from pydub import AudioSegment
from pydub.utils import make_chunks
from pathlib import Path
import traceback

def convert_with_ffmpeg(input_path, output_path):
    """Конвертация аудио с помощью ffmpeg"""
    try:
        command = [
            'ffmpeg',
            '-i', input_path,
            '-ac', '1',  # конвертируем в моно
            '-ar', '16000',  # частота дискретизации 16kHz
            '-strict', '-2',  # разрешаем экспериментальные кодеки
            '-loglevel', 'error',  # уменьшаем вывод ffmpeg
            output_path,
            '-y'  # разрешаем перезапись файла
        ]
        subprocess.run(command, check=True, capture_output=True, text=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"FFmpeg error: {e.stderr}")
        return False
    except Exception as e:
        print(f"Conversion error: {e}")
        return False

def convert_m4a_to_wav(m4a_path, temp_dir):
    """Конвертация M4A в WAV"""
    wav_filename = Path(m4a_path).stem + "_converted.wav"
    wav_path = os.path.join(temp_dir, wav_filename)

    print(f"Конвертация {Path(m4a_path).name} в WAV...")

    # Пробуем сначала стандартный метод pydub
    try:
        audio = AudioSegment.from_file(m4a_path, format="m4a")
        audio = audio.set_channels(1).set_frame_rate(16000)
        audio.export(wav_path, format="wav", parameters=["-ar", "16000"])
        print("  Конвертация завершена с помощью pydub")
        return wav_path
    except Exception as e:
        print(f"  Pydub конвертация не удалась: {e}")
        print("  Пробуем альтернативную конвертацию с ffmpeg...")

        if convert_with_ffmpeg(m4a_path, wav_path):
            print("  Конвертация завершена с помощью ffmpeg")
            return wav_path
        else:
            print("  Оба метода конвертации не сработали")
            return None


def transcribe_audio(wav_path):
    """Транскрибация аудио файла"""
    recognizer = sr.Recognizer()
    recognized_text = ""

    try:
        print("  Загрузка и разделение аудио...")
        audio = AudioSegment.from_file(wav_path, format="wav")

        # Длина фрагмента в миллисекундах (30 секунд)
        chunk_length_ms = 30 * 1000
        chunks = make_chunks(audio, chunk_length_ms)
        print(f"  Разделено на {len(chunks)} фрагментов")

        # Обработка каждого фрагмента
        for i, chunk in enumerate(chunks):
            try:
                chunk_file = f"chunk_{i}.wav"
                chunk.export(chunk_file, format="wav")
                print(f"  Обработка фрагмента {i + 1}/{len(chunks)}...")

                with sr.AudioFile(chunk_file) as source:
                    audio_data = recognizer.record(source)
                    text = recognizer.recognize_google(audio_data, language="ru-RU")
                    recognized_text += text + " "

                # Удаляем временный файл фрагмента
                if os.path.exists(chunk_file):
                    os.remove(chunk_file)

            except sr.UnknownValueError:
                print(f"  Фрагмент {i + 1} не распознан.")
            except sr.RequestError as e:
                print(f"  Ошибка подключения к API: {e}")
            except Exception as e:
                print(f"  Ошибка при обработке фрагмента {i + 1}: {e}")

    except Exception as e:
        print(f"  Ошибка при обработке аудио: {e}")
        print(traceback.format_exc())

    return recognized_text.strip()


def process_all_m4a_files(input_folder):
    """Обработка всех M4A файлов в папке"""
    # Создаем временную папку для конвертированных файлов
    temp_dir = os.path.join(input_folder, "temp_converted")
    os.makedirs(temp_dir, exist_ok=True)

    # Находим все M4A файлы
    m4a_files = []
    for file in os.listdir(input_folder):
        if file.lower().endswith('.m4a'):
            m4a_files.append(os.path.join(input_folder, file))

    if not m4a_files:
        print(f"В папке {input_folder} не найдено файлов .m4a")
        return

    print(f"Найдено {len(m4a_files)} M4A файлов для обработки")

    # Обрабатываем каждый файл
    for m4a_path in m4a_files:
        try:
            print(f"\n{'=' * 50}")
            print(f"Обработка файла: {Path(m4a_path).name}")

            # Конвертируем M4A в WAV
            wav_path = convert_m4a_to_wav(m4a_path, temp_dir)
            if not wav_path or not os.path.exists(wav_path):
                print(f"  Пропускаем {Path(m4a_path).name} - ошибка конвертации")
                continue

            # Транскрибируем аудио
            print("  Начинаю транскрибацию...")
            text = transcribe_audio(wav_path)

            # Сохраняем результат в текстовый файл
            output_filename = Path(m4a_path).stem + "_transcribed.txt"
            output_path = os.path.join(input_folder, output_filename)

            with open(output_path, "w", encoding="utf-8") as file:
                file.write(text)

            print(f"  Текст сохранен в: {output_filename}")
            print(f"  Длина текста: {len(text)} символов")

            # Удаляем временный WAV файл
            if os.path.exists(wav_path):
                os.remove(wav_path)

        except Exception as e:
            print(f"  Ошибка при обработке файла {Path(m4a_path).name}: {e}")
            print(traceback.format_exc())

    # Удаляем временную папку если она пуста
    try:
        if os.path.exists(temp_dir) and not os.listdir(temp_dir):
            os.rmdir(temp_dir)
    except:
        pass

    print(f"\n{'=' * 50}")
    print("Обработка всех файлов завершена!")


def main():
    # Укажите папку с аудиофайлами
    input_folder = "C:\\Users\\Maks\\Downloads\\Telegram Desktop"

    # Проверяем существование папки
    if not os.path.exists(input_folder):
        print(f"Папка {input_folder} не найдена. Проверьте путь.")
        return

    # Проверяем наличие необходимых библиотек
    try:
        import pydub
        import speech_recognition
    except ImportError as e:
        print(f"Ошибка импорта: {e}")
        print("Установите необходимые библиотеки:")
        print("pip install pydub speechrecognition")
        print("Также убедитесь, что ffmpeg установлен и доступен в PATH")
        return

    # Обрабатываем все файлы
    process_all_m4a_files(input_folder)


if __name__ == "__main__":
    main()