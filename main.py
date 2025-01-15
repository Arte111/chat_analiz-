import json
import os
from datetime import datetime, timedelta

import matplotlib.pyplot as plt

path = "result.json"
days_per_period = 30

with open(path, 'r', encoding='utf-8') as file:
    data = json.load(file)
messages = data["messages"]
print(f"час с {data["name"]}")

from collections import defaultdict

message_count = defaultdict(int)

not_message = 0

for message in messages:
    try:
        sender = message['from']
        message_count[sender] += 1
    except KeyError:
        not_message += 1

for user, count in message_count.items():
    print(f"{user}: {count} сообщений")

print(f'не обработано {not_message} сообщений ')


def parse_date(date_str):
    return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")


def count_words(text):
    if isinstance(text, list):
        text = ' '.join(item['text'] if isinstance(item, dict) else item for item in text)
    return len(text.split())


messages_sorted = sorted(messages, key=lambda x: parse_date(x['date']))
start_date = parse_date(messages_sorted[0]['date'])
periods = []
period_counts = []

current_period = []
current_period_end = start_date + timedelta(days=days_per_period)
unrecognized_voice_count = 0

for message in messages_sorted:
    message_date = parse_date(message['date'])

    if message_date < current_period_end:
        current_period.append(message)
    else:
        periods.append(current_period)
        period_count = defaultdict(lambda: {'messages': 0, 'words': 0})
        for msg in current_period:
            try:
                sender = msg['from']
                period_count[sender]['messages'] += 1
                if msg.get("media_type") == "voice_message":
                    # это голосовое сообщение
                    voice_path = msg['file'].replace("voice_messages", "voice_messages_txt").replace(".ogg", ".txt")
                    if os.path.isfile(voice_path):
                        with open(voice_path, 'r', encoding='utf-8') as file:
                            voice_content = file.read()
                        period_count[sender]['words'] += count_words(voice_content)
                    else:
                        unrecognized_voice_count += 1
                else:
                    # это обычное сообщение
                    period_count[sender]['words'] += count_words(msg['text'])
            except KeyError:
                pass

        period_counts.append(period_count)
        current_period = [message]
        current_period_end = current_period_end + timedelta(days=days_per_period)

print(f"проигнорировано {unrecognized_voice_count} голосовых")
period_labels = [start_date + timedelta(days=days_per_period * i) for i in range(len(period_counts))]

users = set()
for period_count in period_counts:
    users.update(period_count.keys())

data_messages = {user: [] for user in users}
data_words = {user: [] for user in users}
for period_count in period_counts:
    for user in users:
        data_messages[user].append(period_count.get(user, {}).get('messages', 0))
        data_words[user].append(period_count.get(user, {}).get('words', 0))

plt.figure(figsize=(12, 6))

for user, counts in data_messages.items():
    plt.plot(period_labels, counts, label=f'{user} - Сообщения', marker='o')

for user, counts in data_words.items():
    plt.plot(period_labels, counts, label=f'{user} - Слова', linestyle='--', marker='.')

plt.xticks(period_labels, [date.strftime('%Y-%m-%d') for date in period_labels], rotation=45)

plt.xlabel('Дата начала периода')
plt.ylabel('Количество')
plt.title('Количество сообщений и слов от каждого пользователя по периодам')
plt.legend()
plt.tight_layout()
plt.show()
