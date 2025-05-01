import csv
import os
from pathlib import Path


current_path = Path(__file__)
current_path = f"{current_path.parent}"
current_path = current_path.replace("\\", "/")
print(f"current_path: {current_path}")

new_row = {
    'name': 'Петров Петр',
    'email': 'petrov@example.com',
    'city': 'Санкт-Петербург'
}

csv_file = f'{current_path}/files/clients.csv'
fieldnames = ['name', 'email', 'city']

file_exists = os.path.isfile(csv_file)

with open(csv_file, mode='a', newline='', encoding='utf-8') as file:
    writer = csv.DictWriter(file, fieldnames=fieldnames)

    # Если файл только создаётся — добавляем заголовки
    if not file_exists:
        writer.writeheader()

    writer.writerow(new_row)

print("Файл создан (если небыло) и строка добавлена.")