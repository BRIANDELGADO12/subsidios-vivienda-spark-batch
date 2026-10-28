# -*- coding: utf-8 -*-
# Consumidor Spark Structured Streaming: lee subsidios desde Kafka y calcula
# estadísticas por ventana de 1 minuto.
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, DoubleType)

TOPIC = 'subsidios_topic'
BOOTSTRAP = 'localhost:9092'
AGRUPAR_POR_PROGRAMA = True   # False = una sola fila por ventana

spark = (SparkSession.builder
         .appName('SubsidiosStreaming')
         .config('spark.sql.shuffle.partitions', '4')   # por defecto son 200 (lento en una VM pequeña)
         .getOrCreate())
spark.sparkContext.setLogLevel('WARN')

# 1. Esquema JSON de los eventos
schema = StructType([
    StructField('departamento', StringType()),
    StructField('municipio', StringType()),
    StructField('programa', StringType()),
    StructField('anio_asignacion', IntegerType()),
    StructField('estado_postulacion', StringType()),
    StructField('hogares', IntegerType()),
    StructField('valor_asignado', DoubleType()),
    StructField('fecha_evento', StringType()),
])

# 2. Lectura del stream desde Kafka
raw = (spark.readStream
       .format('kafka')
       .option('kafka.bootstrap.servers', BOOTSTRAP)
       .option('subscribe', TOPIC)
       .option('startingOffsets', 'latest')
       .load())

# 3. Parseo del JSON y conversión de la fecha del evento
eventos = (raw
           .select(F.from_json(F.col('value').cast('string'), schema).alias('d'))
           .select('d.*')
           .withColumn('fecha_evento', F.to_timestamp('fecha_evento', 'yyyy-MM-dd HH:mm:ss'))
           .dropna(subset=['fecha_evento', 'programa', 'hogares', 'valor_asignado']))

# 4. Estadísticas por ventana de 1 minuto
claves = [F.window('fecha_evento', '1 minute')]
if AGRUPAR_POR_PROGRAMA:
    claves.append(F.col('programa'))

estadisticas = (eventos
                .withWatermark('fecha_evento', '2 minutes')
                .groupBy(*claves)
                .agg(F.count('*').alias('num_subsidios'),
                     F.sum('hogares').alias('total_hogares'),
                     F.round(F.avg('valor_asignado'), 2).alias('promedio_valor_asignado')))

salida = estadisticas.select(
    F.col('window.start').alias('ventana_inicio'),
    F.col('window.end').alias('ventana_fin'),
    *(['programa'] if AGRUPAR_POR_PROGRAMA else []),
    'num_subsidios', 'total_hogares', 'promedio_valor_asignado')


# 5. Salida por consola, ordenada y con formato legible
def mostrar_lote(df, batch_id):
    if df.isEmpty():
        return
    columnas = [F.date_format('ventana_inicio', 'HH:mm').alias('desde'),
                F.date_format('ventana_fin', 'HH:mm').alias('hasta')]
    if AGRUPAR_POR_PROGRAMA:
        columnas.append(F.col('programa'))
    columnas += [F.col('num_subsidios').alias('subsidios'),
                 F.col('total_hogares').alias('hogares'),
                 F.format_number('promedio_valor_asignado', 0).alias('prom_valor_COP')]
    tabla = (df.orderBy(F.col('ventana_inicio').desc(), F.col('total_hogares').desc())
               .select(*columnas))
    print('\n=== Lote %d ===' % batch_id)
    tabla.show(30, truncate=False)


query = (salida.writeStream
         .outputMode('update')
         .foreachBatch(mostrar_lote)
         .trigger(processingTime='10 seconds')
         .start())

query.awaitTermination()
