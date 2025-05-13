import random
from bs4 import BeautifulSoup
import os

with open("diccionarios/diccionario.txt", encoding="utf-8") as f:
    html_content = f.read()

soup = BeautifulSoup(html_content, "html.parser")
words = soup.get_text().split()

target_size = 1 * 1024**3

output_files = [f"../data/n_{i}_archivo_1gb.txt" for i in range(18)]
print(f"Creando {len(output_files)} archivos de 1GB...")
for output_file in output_files:
    buffer_size = 0
    print(f"Creando {output_file}...")
    with open(output_file, "wb") as f:
        while buffer_size < target_size:
            sentence = " ".join(random.choices(words, k=random.randint(8, 15))) + "\n"
            encoded = sentence.encode("utf-8")
            f.write(encoded)
            buffer_size += len(encoded)

for file in output_files:
    size_mb = os.path.getsize(file) / (1024**2)
    print(f"{file} → {size_mb:.2f} MB")