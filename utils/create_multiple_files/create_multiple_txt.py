import random
from bs4 import BeautifulSoup

# Leer el archivo HTML
with open("diccionario.txt", encoding="utf-8") as f:
    html_content = f.read()

# Usar BeautifulSoup para parsear el HTML
soup = BeautifulSoup(html_content, "html.parser")

# Extraer todas las palabras del texto (sin etiquetas HTML)
words = soup.get_text().split()

# Tamaño objetivo en bytes (18 GB)
target_size = 18 * 1024**3

output_files = []
# Crear archivos de 1 GB cada uno
for i in range(18):
    output_file = f"n_{i}_archivo_1gb.txt"
    output_files.append(output_file)
    
buffer_size = 0

for output_file in output_files:
    # Crear un archivo de 1 GB
    with open(output_file, "wb") as f:
        while buffer_size < target_size / len(output_files):
            # Generar una oración aleatoria con 8 a 15 palabras
            sentence = " ".join(random.choices(words, k=random.randint(8, 15))) + "\n"
            f.write(sentence.encode("utf-8"))
            buffer_size += len(sentence.encode("utf-8"))