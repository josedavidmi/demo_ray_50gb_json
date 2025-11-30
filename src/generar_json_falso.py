import os
import json
import time
from faker import Faker
import random

# ---------------- CONFIGURACIÓN ----------------

# Ruta de salida (ajústala a la ruta de tu USB)
OUTPUT_PATH = r"E:\datos\archivo50G.json"

# Tamaño objetivo aproximado (en GB)
TARGET_SIZE_GB = 1  # pon 50 cuando ya lo tengas probado
# ------------------------------------------------


def generar_registro(fake: Faker) -> dict:
    """Devuelve un diccionario con datos ficticios para una línea JSON."""
    return {
        "id": fake.uuid4(),
        "nombre": fake.name(),
        "email": fake.email(),
        "fecha_registro": fake.iso8601(),
        "pais": fake.country(),
        "x": random.uniform(-1000, 1000),
        "y": random.uniform(-1000, 1000),
        "comentario": fake.text(max_nb_chars=200),
    }


def main():
    fake = Faker()
    target_bytes = int(TARGET_SIZE_GB * (1024**3))  # GB -> bytes

    # Crear carpeta destino si no existe
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    print(f"Generando archivo JSON lines en: {OUTPUT_PATH}")
    print(f"Tamaño objetivo aproximado: {TARGET_SIZE_GB} GB ({target_bytes} bytes)\n")

    start_time = time.time()
    num_registros = 0

    # Abrimos el fichero en modo texto y escribimos línea a línea
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        while True:
            registro = generar_registro(fake)
            linea = json.dumps(registro, ensure_ascii=False)
            f.write(linea + "\n")
            num_registros += 1

            # Cada X registros comprobamos tamaño y mostramos progreso
            if num_registros % 10_000 == 0:
                f.flush()
                os.fsync(f.fileno())

                size = os.path.getsize(OUTPUT_PATH)
                elapsed = time.time() - start_time
                mb = size / (1024**2)
                gb = size / (1024**3)

                print(
                    f"Registros: {num_registros:,} | "
                    f"Tamaño aprox: {mb:,.2f} MB ({gb:.2f} GB) | "
                    f"Tiempo: {elapsed:.1f} s",
                    end="\r",
                )

                if size >= target_bytes:
                    break

    elapsed = time.time() - start_time
    final_size = os.path.getsize(OUTPUT_PATH) / (1024**3)
    print("\n\n¡Terminado!")
    print(f"Registros generados: {num_registros:,}")
    print(f"Tamaño final aprox: {final_size:.2f} GB")
    print(f"Tiempo total: {elapsed:.1f} s")


if __name__ == "__main__":
    main()
