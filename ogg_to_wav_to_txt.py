import os
import subprocess

import speech_recognition as sr

# Папки для исходных .ogg файлов, сконвертированных .wav, и текстовых файлов
input_folder = 'voice_messages'
output_folder = 'voice_messages_wav'
text_folder = 'voice_messages_txt'

# Создаем папки для вывода, если они не существуют
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

if not os.path.exists(text_folder):
    os.makedirs(text_folder)


def convert_ogg_to_wav(ogg_file, wav_file):
    ffmpeg_path = r"ffmpeg.exe"
    command = [ffmpeg_path, "-i", ogg_file, wav_file]
    subprocess.run(command, shell=True)


# Конвертируем .ogg в .wav
for filename in os.listdir(input_folder):
    if os.path.isfile(f"voice_messages_wav/{filename.replace("ogg", "wav")}"):
        print(f"файл {filename} уже переведен в wav")
    elif filename.endswith('.ogg'):
        input_file_path = os.path.join(input_folder, filename)
        try:
            output_file_path = os.path.join(output_folder, os.path.splitext(filename)[0] + '.wav')
            convert_ogg_to_wav(input_file_path, output_file_path)
            print(f'Converted {input_file_path} to {output_file_path}')
        except Exception as e:
            print(f"Error converting {input_file_path}: {e}")
            continue

# Обработка .wav файлов для распознавания речи
r = sr.Recognizer()
for filename in os.listdir(output_folder):
    if os.path.isfile(f"voice_messages_txt/{filename.replace("wav", "txt")}"):
        print(f"файл {filename} уже обработан")
    elif filename.endswith('.wav'):
        wav_file_path = os.path.join(output_folder, filename)
        text_file_path = os.path.join(text_folder, os.path.splitext(filename)[0] + '.txt')

        with sr.AudioFile(wav_file_path) as source:
            audio_data = r.record(source)  # Читаем весь файл

        # Распознаем речь с помощью Google Speech Recognition
        try:
            recognized_text = r.recognize_google(audio_data, language="ru-RU")
            print(f"Recognized text for {filename}: {recognized_text}")

            # Сохраняем распознанный текст в файл .txt
            with open(text_file_path, 'w', encoding='utf-8') as text_file:
                text_file.write(recognized_text)

            print(f"Saved recognized text to {text_file_path}")
        except sr.UnknownValueError:
            print(f"Google Speech Recognition could not understand {filename}")
        except sr.RequestError as e:
            print(f"Google Speech Recognition error for {filename}: {e}")

print('All files processed.')
