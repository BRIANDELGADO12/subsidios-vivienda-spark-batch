# -*- coding: utf-8 -*-
# Tarea: procesamiento batch - Subsidios de Vivienda Asignados (datos.gov.co, h2yr-zfb2)
import re
import unicodedata
from pyspark.sql import SparkSession, functions as F

# 1. Sesión de Spark
spark = SparkSession.builder.appName('TareaSubsidios').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

# 2. Ruta del CSV en HDFS
file_path = 'hdfs://localhost:9000/TareaSubsidios/json.csv'

# 3. Carga infiriendo el esquema
primera = spark.read.text(file_path).limit(1).collect()[0][0].strip()
if primera.startswith('[') or primera.startswith('{'):
    print("Formato detectado: JSON")
    df = spark.read.option('multiLine', 'true').json(file_path)
else:
    print("Formato detectado: CSV")
    df = (spark.read.format('csv')
          .option('header', 'true')
          .option('inferSchema', 'true')
          .option('multiLine', 'true')
          .option('escape', '"')
          .load(file_path))

print("\n=== ESQUEMA ORIGINAL ===")
df.printSchema()

# 4. Normalización de nombres de columna (funciona con encabezados "DEPARTAMENTO",
#    "AÑO DE ASIGNACIÓN", o con los nombres de API "a_o_de_asignaci_n")
def normalizar(nombre):
    n = unicodedata.normalize('NFKD', nombre).encode('ascii', 'ignore').decode().lower()
    return re.sub(r'[^a-z0-9]+', '_', n).strip('_')

df = df.toDF(*[normalizar(c) for c in df.columns])

def buscar(cols, condicion):
    for c in cols:
        if condicion(c):
            return c
    return None

cols = df.columns
mapa = {
    'departamento':      buscar(cols, lambda c: c == 'departamento'),
    'municipio':         buscar(cols, lambda c: c == 'municipio'),
    'programa':          buscar(cols, lambda c: c == 'programa'),
    'anio_asignacion':   buscar(cols, lambda c: 'asignaci' in c and c.startswith(('a_o', 'ano', 'anio'))),
    'estado_postulacion': buscar(cols, lambda c: c.startswith('estado')),
    'hogares':           buscar(cols, lambda c: c == 'hogares'),
    'valor_asignado':    buscar(cols, lambda c: c == 'valor_asignado'),
    'cod_dpto':          buscar(cols, lambda c: 'divipola' in c and 'departamento' in c),
    'cod_mpio':          buscar(cols, lambda c: 'divipola' in c and 'municipio' in c),
}
faltantes = [k for k, v in mapa.items() if v is None and k in
             ('departamento', 'municipio', 'programa', 'anio_asignacion', 'hogares', 'valor_asignado')]
if faltantes:
    raise SystemExit("No se encontraron las columnas: %s. Columnas disponibles: %s" % (faltantes, cols))

df = df.select([F.col(v).alias(k) for k, v in mapa.items() if v is not None])

# 5. Limpieza y transformación
for c in ['departamento', 'municipio', 'programa', 'estado_postulacion']:
    if c in df.columns:
        df = df.withColumn(c, F.upper(F.trim(F.col(c).cast('string'))))

df = (df
      .withColumn('anio_asignacion', F.col('anio_asignacion').cast('int'))
      .withColumn('hogares', F.col('hogares').cast('int'))
      .withColumn('valor_asignado', F.col('valor_asignado').cast('double')))

total_inicial = df.count()
df = (df.dropna(subset=['departamento', 'municipio', 'programa', 'hogares', 'valor_asignado'])
        .dropDuplicates())
df = df.withColumn('valor_por_hogar',
                   F.when(F.col('hogares') > 0, F.col('valor_asignado') / F.col('hogares')))
df.cache()
total_final = df.count()

print("\n=== LIMPIEZA ===")
print("Registros iniciales: %d | Registros tras limpieza: %d" % (total_inicial, total_final))

print("\n=== ESQUEMA LIMPIO ===")
df.printSchema()

print("\n=== PRIMERAS FILAS ===")
df.show(10, truncate=False)

# 6. EDA
print("\n=== RESUMEN ESTADÍSTICO ===")
df.select('anio_asignacion', 'hogares', 'valor_asignado', 'valor_por_hogar').summary().show()

print("\n=== NULOS POR COLUMNA ===")
df.select([F.sum(F.col(c).isNull().cast('int')).alias(c) for c in df.columns]).show()

print("\n=== VALORES DISTINTOS ===")
df.select(
    F.countDistinct('departamento').alias('departamentos'),
    F.countDistinct('municipio').alias('municipios'),
    F.countDistinct('programa').alias('programas'),
    F.min('anio_asignacion').alias('anio_min'),
    F.max('anio_asignacion').alias('anio_max')
).show()

# 7. Consultas: filtros y ordenamientos
print("\n=== TOP 20 REGISTROS POR VALOR ASIGNADO (DESC) ===")
df.orderBy(F.col('valor_asignado').desc()).show(20, truncate=False)

print("\n=== TOP 20 REGISTROS POR HOGARES (DESC) ===")
df.orderBy(F.col('hogares').desc()).show(20, truncate=False)

umbral = df.approxQuantile('valor_asignado', [0.99], 0.01)[0]
print("\n=== REGISTROS CON VALOR ASIGNADO > P99 (%.2f), ORDENADOS DESC ===" % umbral)
(df.filter(F.col('valor_asignado') > umbral)
   .select('departamento', 'municipio', 'programa', 'anio_asignacion', 'hogares', 'valor_asignado')
   .orderBy(F.col('valor_asignado').desc())
   .show(20, truncate=False))

print("\n=== HOGARES Y VALOR TOTAL POR DEPARTAMENTO (DESC POR VALOR) ===")
(df.groupBy('departamento')
   .agg(F.sum('hogares').alias('total_hogares'),
        F.round(F.sum('valor_asignado'), 2).alias('valor_total'))
   .orderBy(F.col('valor_total').desc())
   .show(40, truncate=False))

print("\n=== HOGARES Y VALOR TOTAL POR PROGRAMA ===")
(df.groupBy('programa')
   .agg(F.sum('hogares').alias('total_hogares'),
        F.round(F.sum('valor_asignado'), 2).alias('valor_total'))
   .orderBy(F.col('valor_total').desc())
   .show(40, truncate=False))

print("\n=== EVOLUCIÓN POR AÑO DE ASIGNACIÓN ===")
(df.groupBy('anio_asignacion')
   .agg(F.sum('hogares').alias('total_hogares'),
        F.round(F.sum('valor_asignado'), 2).alias('valor_total'))
   .orderBy('anio_asignacion')
   .show(50, truncate=False))

if 'estado_postulacion' in df.columns:
    print("\n=== CONTEO POR ESTADO DE POSTULACIÓN ===")
    df.groupBy('estado_postulacion').count().orderBy(F.col('count').desc()).show(truncate=False)

# 8. Consulta con Spark SQL
df.createOrReplaceTempView('subsidios')
print("\n=== SPARK SQL: TOP 10 MUNICIPIOS POR VALOR TOTAL ===")
spark.sql("""
    SELECT departamento, municipio,
           SUM(hogares) AS total_hogares,
           ROUND(SUM(valor_asignado), 2) AS valor_total
    FROM subsidios
    GROUP BY departamento, municipio
    ORDER BY valor_total DESC
    LIMIT 10
""").show(truncate=False)

# 9. Mantener la sesión abierta para revisar Jobs/Stages en http://IP:4040
input("\nProceso terminado. Revisa la UI en el puerto 4040 y presiona ENTER para cerrar...")
spark.stop()
