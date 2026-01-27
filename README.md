# Distributed Transaction Analytics

Proyecto de ejemplo con Apache Spark que procesa un dataset de transacciones almacenado en formatos CSV y Parquet.

## Requisitos

- Python 3.10 o superior (probado con PySpark 3.5 sobre Windows + Java 11).
- Dependencias: `pip install -r requirements.txt` dentro del entorno virtual.
- Hadoop local (solo `winutils.exe`) apuntando a la ruta indicada en `environment.hadoop_home` del YAML.
- Dataset `dataset_transacciones.csv` o `dataset_transacciones.parquet` ubicado en la raíz del proyecto (se sugiere ≥1 M de registros para reproducir cargas reales).

## Implementación del entorno Spark (3.1)

- **Tipo de ejecución:** local/pseudo-distribuido (`spark.master: local[4]`). El mismo proceso levanta 4 hilos para emular ejecutores.
- **Configuración relevante:**
  - `spark.shuffle.partitions`: número de particiones de barajado (por defecto 8).
  - `spark.input_partitions`: particiones iniciales al leer datasets.
  - `spark.executor.memory`: memoria del proceso driver/ejecutor (4 GB actualmente).
  - `spark.executor.cores`: núcleos usados por tarea (2 actualmente).
  - `spark.local.dir`: definido via `local_temp_dir` para controlar dónde se crean temporales.
- Todas estas llaves viven en [config/app_config.yaml](config/app_config.yaml) y se cargan al construir la sesión en [src/utils/spark_session.py](src/utils/spark_session.py#L8-L32).

## Estructura

```
config/
  app_config.yaml
output/
  curated/
  rejected/
  analytics/
src/
  main.py
  io/
    reader.py
  processing/
    schema.py
    validation.py
  analytics/
    metrics.py
  utils/
    config_loader.py
    spark_session.py
```

## Ejecución

```powershell
# activar entorno
.\.venv\Scripts\activate

# instalar dependencias
pip install -r requirements.txt

# ejecutar (csv o parquet)
python -m src.main --config config/app_config.yaml --format csv
```

- Ajusta `spark` en el YAML para definir maestro, particiones y directorios temporales (`spark.local.dir`).
- Al arrancar la app se imprime la URL del Spark UI (por defecto `http://localhost:4040`) para monitorear jobs y stages.
- Los datos curados y rechazados se escriben en `output/curated` y `output/rejected`; los agregados en `output/analytics`.

## Funciones clave que cubren los requisitos

- **Lectura distribuida:** [read_distributed_dataset](src/io/reader.py#L5-L46) define el esquema y reparte el dataset según el formato (CSV/Parquet).
- **Esquema explícito:** [TRANSACTION_SCHEMA](src/processing/schema.py#L1-L16) modela cada campo con su tipo.
- **Validación de nulos y tipos:** [run_quality_checks](src/main.py#L56-L81) invoca [detect_nulls](src/processing/validation.py#L7-L13) y [validate_types](src/processing/validation.py#L16-L20).
- **Filtrado de datos inválidos:** [filter_invalid_records](src/processing/validation.py#L23-L32) separa registros limpios y rechazados; se ejecuta desde [main](src/main.py#L143-L161).
- **Persistencia y analítica:** [persist_outputs](src/main.py#L36-L54) y [run_analytics](src/main.py#L83-L138) completan el flujo requerido.

## Verificación funcional básica (4.2)

El pipeline ejecuta un chequeo al final de la corrida:

1. Toma un subconjunto de 1 000 registros.
2. Calcula el total de compras y el ratio de fallos con Spark (`agg`).
3. Colecta la misma muestra a memoria y repite el cálculo manualmente.
4. Imprime ambos resultados para que puedas comparar rápidamente contra cálculos parciales o datasets reducidos.

Adicionalmente puedes validar:

- Reemplazando temporalmente el dataset por una muestra pequeña y re-ejecutando el script.
- Revisando el Spark UI (URL mostrada en consola) para contrastar métricas por Stage y confirmar que los totales coinciden.

## Funcionalidades cubiertas

- Lectura distribuida y esquema explícito (`TRANSACTION_SCHEMA`).
- Validaciones de nulos, tipos y reglas de negocio (`processing/validation.py`).
- Transformaciones `filter`, `groupBy`, `agg`, `reduce` y acciones `count`, `collect`, `show`, `write`.
- Métricas: ventas por región, top 10 productos, porcentaje de fallos y suma total.
- Conteo de fallos por ventana y detección de picos (umbral simple, media±desviación, percentil 95).
