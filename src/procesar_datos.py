import os
import time
import ray
import ray.data as rd
import pandas as pd

# --------------------------------------------------------------------
# 0. Punto de entrada (MUY IMPORTANTE en Windows)
# --------------------------------------------------------------------
if __name__ == "__main__":

    # 1. Inicializar Ray usando todos los núcleos lógicos
    ray.shutdown()
    ray.init(num_cpus=os.cpu_count())
    print("CPUs lógicas:", os.cpu_count())

    # 2. Ruta al JSON gigante en el USB
    ruta_archivo = r"E:\datos\archivo50G.json"  # ajusta esto a tu ruta real

    # 3. Leer el JSON como Ray Dataset (NO se carga todo en memoria de golpe)
    inicio = time.time()
    ds = rd.read_json(ruta_archivo)   # Ray se encarga de partir el fichero en bloques
    fin = time.time()
    print("Dataset leído.")
    print(ds)
    print(f"Tiempo de read_json: {fin - inicio:.2f} s")

    # 4. Ver unas pocas filas para inspeccionar (no carga todo)
    print("\nPrimeras filas:")
    ds.show(5)

    # 5. Definir una función de transformación por batch (pandas DataFrame)
    def enrich_batch(batch: pd.DataFrame) -> pd.DataFrame:
        # Importante: trabajar sobre una copia si vamos a añadir columnas
        batch = batch.copy()

        # Ejemplos de transformación:
        # - Nueva columna z = x^2 + y^2 (como antes)
        if "x" in batch.columns and "y" in batch.columns:
            batch["z"] = batch["x"]**2 + batch["y"]**2

            # por ejemplo, media simple de x e y
            batch["xy_mean"] = (batch["x"] + batch["y"]) / 2

        # Puedes añadir más lógica, filtros, etc.
        return batch

    # 6. Aplicar la transformación en paralelo con Ray sobre todos los datos
    inicio = time.time()
    ds_enriched = ds.map_batches(
        enrich_batch,
        batch_format="pandas",  # cada batch será un DataFrame de pandas
    )
    fin = time.time()
    print(f"\nTransformación paralela completada en: {fin - inicio:.2f} s")
    print(ds_enriched)

    # 7. Ejemplo de agregados globales: media de alguna columna
    if "z" in ds_enriched.schema().names:
        mean_z = ds_enriched.mean("z")
        print(f"\nMedia global de z: {mean_z}")

    if "xy_mean" in ds_enriched.schema().names:
        mean_xy_mean = ds_enriched.mean("xy_mean")
        print(f"Media global de xy_mean: {mean_xy_mean}")

    # 8. Guardar el resultado enriquecido en disco de forma particionada (parquet)
    salida_dir = r"E:\datos\salida_enriquecida"  # o en tu disco interno, mejor que en el USB
    os.makedirs(salida_dir, exist_ok=True)

    inicio = time.time()
    ds_enriched.write_parquet(salida_dir)
    fin = time.time()
    print(f"\nDatos enriquecidos guardados en: {salida_dir}")
    print(f"Tiempo de escritura parquet: {fin - inicio:.2f} s")

    ray.shutdown()
