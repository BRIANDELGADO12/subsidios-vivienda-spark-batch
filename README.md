# Subsidios de Vivienda Asignados: procesamiento batch (Spark/HDFS) y tiempo real (Kafka/Streaming)

Proyecto del curso **Big Data** (Ingeniería de Sistemas, UNAD). Analiza el conjunto de datos abierto *Subsidios de Vivienda Asignados* de Colombia con dos enfoques:

- **Batch:** limpieza, transformación y análisis exploratorio con **Apache Spark (PySpark)** sobre **HDFS**.
- **Tiempo real:** simulación de la llegada de subsidios con **Apache Kafka** y cálculo de estadísticas por ventana con **Spark Structured Streaming**.

## 1. Descripción de la solución

### Procesamiento batch (`src/subsidios.py`)

1. **Ingesta:** el dataset se descarga desde datos.gov.co y se carga en HDFS (`/TareaSubsidios`).
2. **Lectura:** Spark lee `hdfs://localhost:9000/TareaSubsidios/json.csv`. El script detecta si el archivo es **JSON** (respuesta de la API de Socrata) o **CSV** y normaliza los nombres de columna.
3. **Limpieza y transformación:** conversión de tipos, `trim` y mayúsculas, eliminación de nulos y duplicados exactos, y columna derivada `valor_por_hogar`. Cada decisión, su justificación y su efecto medido están en [`docs/decisiones_limpieza.md`](docs/decisiones_limpieza.md).
4. **Análisis exploratorio (EDA):** estadísticas descriptivas, cobertura, cifras clave, rankings por departamento, municipio y programa (con participación porcentual del valor y de los hogares), evolución anual, estados del subsidio, valores extremos (percentil 99), consulta en Spark SQL y hallazgos calculados automáticamente. Genera cuatro gráficas en `resultados_subsidios/`.

### Tiempo real (`streaming/`)

1. **Productor (`subsidios_producer.py`):** toma registros reales del dataset al azar, les agrega `fecha_evento` y envía **un evento por segundo** al topic `subsidios_topic`.
2. **Kafka:** ZooKeeper y broker en `localhost:9092`.
3. **Consumidor (`subsidios_streaming_consumer.py`):** Spark Structured Streaming lee el topic, parsea el JSON y calcula, por **ventana de 1 minuto** y programa, el número de subsidios, la suma de hogares y el valor promedio asignado. Usa *watermark* de 2 minutos, modo `update` y un lote cada 10 segundos.

### Dataset

| Dato | Valor |
|---|---|
| Nombre | Subsidios de Vivienda Asignados |
| Fuente | [datos.gov.co](https://www.datos.gov.co/Vivienda-Ciudad-y-Territorio/Subsidios-De-Vivienda-Asignados/h2yr-zfb2) (identificador `h2yr-zfb2`) |
| Registros | 87.095 en la carga analizada (2003–2026) |
| Columnas usadas | `departamento`, `municipio`, `programa`, `anio_asignacion`, `estado_postulacion`, `hogares`, `valor_asignado` (más los códigos DIVIPOLA) |

El dataset **no se incluye en el repositorio** (ver `.gitignore`); se descarga con `scripts/cargar_hdfs.sh`.

## 2. Resultados principales (batch)

Sobre 56.720 registros válidos de 87.095 leídos (65,1 % conservado tras eliminar duplicados exactos):

| Indicador | Valor |
|---|---|
| Departamentos / municipios / programas | 33 / 991 / 26 |
| Hogares reportados | 940.626 |
| Valor total asignado | $19,0 billones COP |
| Valor promedio por hogar | $20,2 millones COP |
| Departamento con mayor monto | Valle del Cauca (14,52 % del valor) |
| Municipio con mayor monto | Bogotá, D.C. (5,44 % del valor) |
| Programa con mayor valor | Mi Casa Ya (41,89 % del valor) |

Las gráficas generadas están en [`docs/evidencias/`](docs/evidencias/).

> «Hogares reportados» es la suma de la columna `hogares`; no equivale necesariamente a hogares únicos. Los valores exactos pueden variar si el dataset se descarga en otra fecha, porque datos.gov.co lo actualiza.

## 3. Estructura del repositorio

```
.
├── README.md
├── requirements.txt
├── .gitignore
├── src/
│   └── subsidios.py                      # Procesamiento batch (PySpark)
├── streaming/
│   ├── subsidios_producer.py             # Productor Kafka (simula eventos)
│   └── subsidios_streaming_consumer.py   # Consumidor Spark Structured Streaming
├── scripts/
│   └── cargar_hdfs.sh                    # Descarga el dataset y lo carga en HDFS
└── docs/
    ├── decisiones_limpieza.md            # Metodología: decisiones de limpieza y transformación
    └── evidencias/                       # Gráficas y capturas (Spark UI, consola)
```

## 4. Requisitos

- Ubuntu 22.04 (máquina virtual en VirtualBox, según el instructivo del curso)
- Java 11
- Apache Hadoop 3.3.x con HDFS en `localhost:9000`
- Apache Spark 3.5.x en `/opt/spark`, con `SPARK_HOME` y `PATH` configurados
- Apache Kafka 3.x en `/opt/Kafka` (solo para la parte de tiempo real)
- Python 3.10 y las librerías de `requirements.txt` (`pip install -r requirements.txt`)

## 5. Ejecución del procesamiento batch

### Paso 1. Iniciar Spark

```bash
start-master.sh
sudo ss -tunelp | grep 8080
start-worker.sh spark://bigdata:7077
```

Verificar en `http://<IP_VM>:8080` que haya un worker en estado `ALIVE`.

### Paso 2. Iniciar Hadoop y cargar el dataset en HDFS

Con el usuario `hadoop`:

```bash
start-all.sh
bash scripts/cargar_hdfs.sh
```

El script crea `/TareaSubsidios`, descarga el archivo y lo copia a HDFS. Equivale a:

```bash
hdfs dfs -mkdir -p /TareaSubsidios
wget -O json.csv "https://www.datos.gov.co/api/views/h2yr-zfb2/rows.csv?accessType=DOWNLOAD"
hdfs dfs -put -f json.csv /TareaSubsidios
hdfs dfs -ls /TareaSubsidios
```

### Paso 3. Ejecutar

```bash
python3 src/subsidios.py
```

O enviándolo al clúster Spark standalone:

```bash
spark-submit --master spark://bigdata:7077 src/subsidios.py
```

### Paso 4. Monitorear

Mientras corre, Spark UI está en `http://<IP_VM>:4040` (pestañas Jobs, Stages, SQL / DataFrame). El script espera un `ENTER` al final para dar tiempo de tomar las capturas.

## 6. Ejecución del análisis en tiempo real

Abrir una terminal por cada paso que quede en ejecución.

**1. Iniciar ZooKeeper y Kafka** (en segundo plano):

```bash
cd /opt/Kafka
bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
bin/kafka-server-start.sh -daemon config/server.properties
```

**2. Crear el topic** (una sola vez):

```bash
bin/kafka-topics.sh --create --topic subsidios_topic \
  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

**3. Consumidor con Spark** (terminal 1):

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  streaming/subsidios_streaming_consumer.py
```

**4. Productor** (terminal 2), indicando el archivo local del dataset (JSON o CSV):

```bash
python3 streaming/subsidios_producer.py /ruta/al/dataset.json
```

La tabla del consumidor se actualiza cada 10 segundos con el conteo de subsidios, los hogares y el valor promedio por ventana de 1 minuto y programa. Spark UI (puerto 4040, pestaña *Structured Streaming*) muestra el ritmo de entrada y los lotes.

> Las horas de las ventanas se muestran en UTC (cinco horas por delante de Colombia).

## 7. Salidas del script batch

- Esquema original y esquema limpio; registros antes y después de la limpieza
- Estadísticas descriptivas, nulos por columna y cobertura
- Cifras clave (registros, hogares, valor total, valor promedio por hogar, periodo)
- Top 10 de departamentos, top 15 de municipios y distribución por programa, con participación porcentual
- Evolución anual y registros por estado de postulación
- Top 10 de registros por valor y por número de hogares
- Registros por encima del percentil 99 de `valor_asignado`
- Hallazgos principales calculados con los datos y consulta en Spark SQL
- Gráficas PNG en `resultados_subsidios/`

## 8. Notas

- El nombre `json.csv` se conserva por la ruta usada en el curso; el contenido puede ser JSON o CSV y el script lo detecta.
- La eliminación de duplicados exactos reduce cerca de 7 % los hogares y el valor frente a conservarlos; el análisis de sensibilidad está en `docs/decisiones_limpieza.md`.
- El flujo de tiempo real es una **simulación**: reutiliza registros históricos con la hora actual como fecha del evento.
- No se incluyen credenciales ni direcciones IP de la máquina virtual en este repositorio.

## 9. Evidencias

En `docs/evidencias/` están las gráficas generadas por el script. Agregar ahí las capturas de la ejecución (Spark UI, resultados en consola, carpeta en HDFS, productor y consumidor).

## 10. Autor

Brian, Ingeniería de Sistemas, UNAD. Curso Big Data (código 202016911).

## 11. Referencias

- Datos Abiertos Colombia. (s. f.). *Subsidios de vivienda asignados* [Conjunto de datos]. https://www.datos.gov.co/Vivienda-Ciudad-y-Territorio/Subsidios-De-Vivienda-Asignados/h2yr-zfb2
- Apache Software Foundation. (s. f.). *Apache Spark documentation*. https://spark.apache.org/docs/latest/
- Apache Software Foundation. (s. f.). *Structured Streaming programming guide*. https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
- Apache Software Foundation. (s. f.). *Apache Kafka documentation*. https://kafka.apache.org/documentation/
