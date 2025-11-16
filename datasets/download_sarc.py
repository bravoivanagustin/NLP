import os
import json
import requests
import csv
import gzip
import bz2
from tqdm import tqdm

# === CONFIGURACIÓN ===
SARC_URL = "https://nlp.cs.princeton.edu/old/SARC/2.0/main/comments.json.bz2"
OUT_COMPRESSED = "sarc.json.bz2"
OUT_JSON = "sarc.json"
CLEAN_FILE = "sarc_clean.csv"
MIN_LEN = 10  # longitud mínima de comentario

# === DESCARGA ===
def download_file(url, filename):
    if os.path.exists(filename) and os.path.getsize(filename) > 0:
        print(f"[✔] Archivo {filename} ya existe. Saltando descarga.")
        return
    print(f"[↓] Descargando {filename}...")
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(filename, "wb") as f:
            for chunk in tqdm(r.iter_content(chunk_size=8192)):
                if chunk:
                    f.write(chunk)
    print(f"[✔] Descarga completa: {filename}")

# === DESCOMPRESIÓN AUTOMÁTICA ===
def decompress_auto(infile, outfile):
    if os.path.exists(outfile):
        print(f"[✔] Archivo {outfile} ya existe. Saltando descompresión.")
        return
    print(f"[⚙] Descomprimiendo {infile}...")

    opener = None
    if infile.endswith(".gz"):
        opener = gzip.open
    elif infile.endswith(".bz2"):
        opener = bz2.open
    else:
        raise ValueError("Formato de compresión no soportado (usa .gz o .bz2).")

    with opener(infile, "rb") as f_in, open(outfile, "wb") as f_out:
        for line in tqdm(f_in):
            f_out.write(line)
    print(f"[✔] Descompresión completa: {outfile}")

# === LIMPIEZA Y GUARDADO A CSV ===
def clean_to_csv(infile, outfile, min_len=MIN_LEN):
    print(f"[🧹] Limpiando dataset y exportando a CSV...")

    with open(infile, "r", encoding="utf-8") as fin, open(outfile, "w", newline='', encoding="utf-8") as fout:
        writer = csv.writer(fout)
        writer.writerow(["text", "label"])  # cabecera

        for line in tqdm(fin):
            try:
                data = json.loads(line)
                text = data.get("comment", "").strip()
                label = data.get("label")
                if len(text) >= min_len and label in [0, 1]:
                    writer.writerow([text.replace("\n", " "), label])
            except json.JSONDecodeError:
                continue

    print(f"[✔] Archivo limpio guardado en {outfile}")

# === PIPELINE ===
if __name__ == "__main__":
    download_file(SARC_URL, OUT_COMPRESSED)
    decompress_auto(OUT_COMPRESSED, OUT_JSON)
    clean_to_csv(OUT_JSON, CLEAN_FILE)
    print("\nListo. 'sarc_clean.csv' contiene las columnas text,label. Disfrutá tu porción de sarcasmo redditiano.")
