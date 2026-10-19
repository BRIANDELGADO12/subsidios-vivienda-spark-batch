# Procesamiento batch de Subsidios de Vivienda Asignados con Apache Spark y Hadoop

Proyecto del curso **Big Data** (Ingeniería de Sistemas, UNAD). Implementa un flujo de procesamiento por lotes (*batch*) con **Apache Spark (PySpark)** sobre **HDFS** para limpiar, transformar y analizar el conjunto de datos abierto *Subsidios de Vivienda Asignados* de Colombia.

## 1. Descripción de la solución

El flujo tiene cuatro etapas:

1. **Ingesta:** el dataset se descarga desde datos.gov.co y se carga en HDFS (`/TareaSubsidios`).
2. **Lectura:** Spark lee el archivo desde `hdfs://localhost:9000/TareaSubsidios/json.csv`. El script detecta automáticamente si el archivo es **JSON** (respuesta de la API de Socrata) o **CSV**.
3. **Limpieza y transformación:**
   - Normaliza los nombres de columna (sin tildes ni espacios) y descarta las columnas de sistema de Socrata (`:id`, `:version`, `:created_at`, `:updated_at`).
   - Aplica `trim` y mayúsculas a los campos de texto.
   - Convierte `hogares` a entero y `valor_asignado` a decimal.
   - Elimina registros con nulos en campos clave y duplicados.
   - Calcula la columna derivada `valor_por_hogar`.
4. **Análisis exploratorio (EDA):** esquema, primeras filas, estadísticas descriptivas (`summary()`), nulos por columna, conteos distintos, filtros, ordenamientos descendentes, agregaciones por departamento, programa y año, y una consulta en Spark SQL.

### Dataset

| Dato | Valor |
|---|---|
| Nombre | Subsidios de Vivienda Asignados |
| Fuente | [datos.gov.co](https://www.datos.gov.co) (identificador `h2yr-zfb2`) |
| Registros | ~87.000 |
| Columnas usadas | `departamento`, `municipio`, `programa`, `anio_asignacion`, `estado_postulacion`, `hogares`, `valor_asignado` (más los códigos DIVIPOLA) |

El dataset **no se incluye en el repositorio** (ver `.gitignore`); se descarga con el script de `scripts/`.

## 2. Estructura del repositorio

```
.
├── README.md
├── requirements.txt
├── .gitignore
├── src/
│   └── subsidios.py              # Script PySpark (procesamiento batch)
├── scripts/
│   └── cargar_hdfs.sh            # Descarga el dataset y lo carga en HDFS
└── docs/
    └── evidencias/               # Capturas de pantalla (Spark UI, resultados)
```

## 3. Requisitos

- Ubuntu 22.04 (máquina virtual en VirtualBox, según el instructivo del curso)
- Java 11
- Apache Hadoop 3.3.x con HDFS en `localhost:9000`
- Apache Spark 3.5.x instalado en `/opt/spark`, con `SPARK_HOME` y `PATH` configurados
- Python 3.10 y `pyspark` (ver `requirements.txt`)

## 4. Instrucciones de ejecución

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

### Paso 3. Ejecutar el procesamiento batch

```bash
python3 src/subsidios.py
```

Alternativa enviándolo al clúster Spark standalone:

```bash
spark-submit --master spark://bigdata:7077 src/subsidios.py
```

### Paso 4. Monitorear

Mientras el script corre, la interfaz de Spark UI está en `http://<IP_VM>:4040` (pestañas Jobs, Stages, SQL / DataFrame). El script espera un `ENTER` al final para dar tiempo de tomar las capturas.

## 5. Salidas del script

- Esquema original y esquema limpio
- Conteo de registros antes y después de la limpieza
- Estadísticas descriptivas de `anio_asignacion`, `hogares`, `valor_asignado` y `valor_por_hogar`
- Top 20 de registros por `valor_asignado` y por `hogares` (orden descendente)
- Registros por encima del percentil 99 de `valor_asignado`
- Hogares y valor total por departamento, programa y año de asignación
- Top 10 de municipios por valor total (Spark SQL)

## 6. Notas

- El nombre `json.csv` se conserva por la ruta usada en el curso; el contenido puede ser JSON o CSV y el script lo detecta.
- El umbral del filtro de valores altos es el percentil 99, para no depender de una cifra fija.
- No se incluyen credenciales ni direcciones IP de la máquina virtual en este repositorio.

## 7. Evidencias

Agregar en `docs/evidencias/` las capturas de la ejecución (Spark UI, resultados en consola, carpeta en HDFS).

## 8. Autor

Brian — Ingeniería de Sistemas, UNAD. Curso Big Data (código 202016911).
