# demo_ray_50gb_json

Demo de cómo procesar un archivo JSON Lines muy grande (decenas de GB)
usando **Ray Datasets** en un entorno local (Windows 11).

## Estructura

- `src/generar_json_falso.py`  
  Genera un archivo JSON Lines grande con datos ficticios usando Faker.

- `src/procesar_con_ray_dataset.py`  
  Lee el JSON como Ray Dataset, aplica transformaciones por *batches* (lotes)
  en paralelo y guarda el resultado en parquet particionado.

- `notebooks/01_demo_ray_dataset.ipynb`  
  Versión interactiva con explicaciones paso a paso.

## Requisitos

```bash
pip install -r requirements.txt
