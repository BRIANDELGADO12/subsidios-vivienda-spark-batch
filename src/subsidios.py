# -*- coding: utf-8 -*-
"""Tarea 3 - Procesamiento batch de subsidios de vivienda con Apache Spark.

Mejoras principales del EDA:
- Resumen general con registros, hogares reportados y valor asignado.
- Rankings agregados por departamento y municipio.
- Distribución por programa con porcentajes del valor y de los hogares.
- Evolución anual y conclusiones automáticas calculadas a partir de los datos.

Nota: los resultados dependen del contenido real del archivo cargado desde HDFS.
"""
import os
import re
import unicodedata
from pyspark.sql import SparkSession, functions as F


# ---------------------------------------------------------------------------
# 1. Sesión de Spark
# ---------------------------------------------------------------------------
spark = SparkSession.builder.appName("TareaSubsidios").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 2. Ruta del archivo en HDFS
file_path = "hdfs://localhost:9000/TareaSubsidios/json.csv"

# ---------------------------------------------------------------------------
# 3. Carga del conjunto de datos (CSV o JSON)
# ---------------------------------------------------------------------------
primera_fila = spark.read.text(file_path).limit(1).collect()
if not primera_fila:
    spark.stop()
    raise SystemExit("El archivo de entrada está vacío: %s" % file_path)

primera = primera_fila[0][0].strip()
if primera.startswith("[") or primera.startswith("{"):
    print("Formato detectado: JSON")
    df = spark.read.option("multiLine", "true").json(file_path)
else:
    print("Formato detectado: CSV")
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "true")
        .option("escape", '"')
        .load(file_path)
    )

print("\n=== ESQUEMA ORIGINAL ===")
df.printSchema()

# ---------------------------------------------------------------------------
# 4. Normalizar nombres y localizar las columnas esperadas
# ---------------------------------------------------------------------------
def normalizar(nombre):
    nombre_ascii = (
        unicodedata.normalize("NFKD", nombre)
        .encode("ascii", "ignore")
        .decode()
        .lower()
    )
    return re.sub(r"[^a-z0-9]+", "_", nombre_ascii).strip("_")


def buscar(columnas, condicion):
    for columna in columnas:
        if condicion(columna):
            return columna
    return None


df = df.toDF(*[normalizar(c) for c in df.columns])
columnas = df.columns
mapa = {
    "departamento": buscar(columnas, lambda c: c == "departamento"),
    "municipio": buscar(columnas, lambda c: c == "municipio"),
    "programa": buscar(columnas, lambda c: c == "programa"),
    "anio_asignacion": buscar(
        columnas,
        lambda c: "asignaci" in c and c.startswith(("a_o", "ano", "anio")),
    ),
    "estado_postulacion": buscar(columnas, lambda c: c.startswith("estado")),
    "hogares": buscar(columnas, lambda c: c == "hogares"),
    "valor_asignado": buscar(columnas, lambda c: c == "valor_asignado"),
    "cod_dpto": buscar(
        columnas, lambda c: "divipola" in c and "departamento" in c
    ),
    "cod_mpio": buscar(
        columnas, lambda c: "divipola" in c and "municipio" in c
    ),
}

campos_obligatorios = (
    "departamento",
    "municipio",
    "programa",
    "anio_asignacion",
    "hogares",
    "valor_asignado",
)
faltantes = [campo for campo in campos_obligatorios if mapa[campo] is None]
if faltantes:
    spark.stop()
    raise SystemExit(
        "No se encontraron las columnas: %s. Columnas disponibles: %s"
        % (faltantes, columnas)
    )

df = df.select(
    [F.col(origen).alias(destino) for destino, origen in mapa.items() if origen]
)

# ---------------------------------------------------------------------------
# 5. Limpieza y transformación
# ---------------------------------------------------------------------------
for columna in ("departamento", "municipio", "programa", "estado_postulacion"):
    if columna in df.columns:
        df = df.withColumn(
            columna, F.upper(F.trim(F.col(columna).cast("string")))
        )

df = (
    df.withColumn("anio_asignacion", F.col("anio_asignacion").cast("int"))
    .withColumn("hogares", F.col("hogares").cast("long"))
    .withColumn("valor_asignado", F.col("valor_asignado").cast("double"))
)

total_inicial = df.count()
df = (
    df.dropna(
        subset=[
            "departamento",
            "municipio",
            "programa",
            "hogares",
            "valor_asignado",
        ]
    )
    # Decision documentada en docs/decisiones_limpieza.md: se eliminan filas identicas en todos los campos.
    .dropDuplicates()
)
df = df.withColumn(
    "valor_por_hogar",
    F.when(F.col("hogares") > 0, F.col("valor_asignado") / F.col("hogares")),
)
df.cache()
total_final = df.count()

print("\n=== LIMPIEZA ===")
print("Registros iniciales: {:,}".format(total_inicial))
print("Registros después de limpiar y eliminar duplicados: {:,}".format(total_final))
print("Registros descartados: {:,}".format(total_inicial - total_final))

if total_final == 0:
    df.unpersist()
    spark.stop()
    raise SystemExit(
        "No quedan registros después de la limpieza. Verifica las columnas y los tipos de datos."
    )

print("\n=== ESQUEMA LIMPIO ===")
df.printSchema()

print("\n=== MUESTRA DE REGISTROS LIMPIOS (10) ===")
df.show(10, truncate=False)

# ---------------------------------------------------------------------------
# 6. Funciones para presentar las tablas con mayor claridad
# ---------------------------------------------------------------------------
def mostrar_tabla(tabla, limite=20, dinero=(), enteros=(), porcentajes=()):
    """Formatea algunas columnas únicamente para mostrarlas en consola."""
    resultado = tabla
    for columna in dinero:
        if columna in resultado.columns:
            resultado = resultado.withColumn(
                columna, F.format_number(F.col(columna), 2)
            )
    for columna in enteros:
        if columna in resultado.columns:
            resultado = resultado.withColumn(
                columna, F.format_number(F.col(columna), 0)
            )
    for columna in porcentajes:
        if columna in resultado.columns:
            resultado = resultado.withColumn(
                columna, F.format_number(F.col(columna), 2)
            )
    resultado.show(limite, truncate=False)


def moneda_co(valor):
    """Convierte un valor numérico a una cadena con formato monetario colombiano."""
    if valor is None:
        valor = 0
    texto = "{:,.2f}".format(float(valor))
    texto = texto.replace(",", "X").replace(".", ",").replace("X", ".")
    return "$ " + texto


def entero_formateado(valor):
    return "{:,.0f}".format(float(valor or 0)).replace(",", ".")


def porcentaje(columna_numerador, denominador):
    """Devuelve el porcentaje y evita divisiones por cero."""
    return F.when(
        F.lit(denominador) != 0,
        F.round(columna_numerador / F.lit(denominador) * 100, 2),
    ).otherwise(F.lit(0.0))


# ---------------------------------------------------------------------------
# 7. Análisis exploratorio: perfil, calidad y cifras generales
# ---------------------------------------------------------------------------
print("\n" + "=" * 78)
print("ANÁLISIS EXPLORATORIO DE DATOS (EDA) - SUBSIDIOS DE VIVIENDA")
print("=" * 78)

print("\n=== RESUMEN ESTADÍSTICO ===")
df.select(
    "anio_asignacion", "hogares", "valor_asignado", "valor_por_hogar"
).summary().show(truncate=False)

print("\n=== NULOS POR COLUMNA DESPUÉS DE LA LIMPIEZA ===")
df.select(
    [F.sum(F.col(c).isNull().cast("long")).alias(c) for c in df.columns]
).show(truncate=False)

print("\n=== COBERTURA DEL CONJUNTO DE DATOS ===")
df.select(
    F.countDistinct("departamento").alias("departamentos_distintos"),
    F.countDistinct("municipio").alias("municipios_distintos"),
    F.countDistinct("programa").alias("programas_distintos"),
    F.min("anio_asignacion").alias("primer_anio"),
    F.max("anio_asignacion").alias("ultimo_anio"),
).show(truncate=False)

resumen = df.agg(
    F.count(F.lit(1)).alias("registros"),
    F.countDistinct("departamento").alias("departamentos"),
    F.countDistinct("municipio").alias("municipios"),
    F.countDistinct("programa").alias("programas"),
    F.sum("hogares").alias("hogares_reportados"),
    F.sum("valor_asignado").alias("valor_total"),
    F.min("anio_asignacion").alias("anio_min"),
    F.max("anio_asignacion").alias("anio_max"),
).first()

total_hogares = int(resumen["hogares_reportados"] or 0)
total_valor = float(resumen["valor_total"] or 0.0)
valor_promedio_hogar = total_valor / total_hogares if total_hogares > 0 else 0.0

print("\n=== CIFRAS CLAVE DEL PROCESAMIENTO BATCH ===")
print("Registros válidos analizados : {:>15}".format(entero_formateado(resumen["registros"])))
print("Departamentos representados  : {:>15}".format(entero_formateado(resumen["departamentos"])))
print("Municipios representados     : {:>15}".format(entero_formateado(resumen["municipios"])))
print("Programas identificados      : {:>15}".format(entero_formateado(resumen["programas"])))
print("Hogares sumados/reportados   : {:>15}".format(entero_formateado(total_hogares)))
print("Valor total asignado         : {:>15}".format(moneda_co(total_valor)))
print("Valor promedio por hogar*    : {:>15}".format(moneda_co(valor_promedio_hogar)))
print("Periodo cubierto             : {} - {}".format(resumen["anio_min"], resumen["anio_max"]))
print("* Calculado como valor total asignado / suma de hogares reportados.")
print(
    "Nota: la suma de hogares corresponde a los valores reportados en los registros. "
    "No representa necesariamente hogares únicos si una misma familia aparece en más de un registro."
)

# Rankings agregados. Se conservan las columnas numéricas para poder ordenar y calcular.
por_departamento = (
    df.groupBy("departamento")
    .agg(
        F.count(F.lit(1)).alias("registros"),
        F.sum("hogares").alias("hogares_reportados"),
        F.sum("valor_asignado").alias("valor_total"),
    )
    .withColumn("participacion_valor_pct", porcentaje(F.col("valor_total"), total_valor))
    .withColumn(
        "participacion_hogares_pct",
        porcentaje(F.col("hogares_reportados"), total_hogares),
    )
    .withColumn(
        "valor_promedio_hogar",
        F.when(
            F.col("hogares_reportados") > 0,
            F.round(F.col("valor_total") / F.col("hogares_reportados"), 2),
        ).otherwise(F.lit(0.0)),
    )
    .orderBy(F.col("valor_total").desc())
    .cache()
)

por_municipio = (
    df.groupBy("departamento", "municipio")
    .agg(
        F.count(F.lit(1)).alias("registros"),
        F.sum("hogares").alias("hogares_reportados"),
        F.sum("valor_asignado").alias("valor_total"),
    )
    .withColumn("participacion_valor_pct", porcentaje(F.col("valor_total"), total_valor))
    .withColumn(
        "participacion_hogares_pct",
        porcentaje(F.col("hogares_reportados"), total_hogares),
    )
    .withColumn(
        "valor_promedio_hogar",
        F.when(
            F.col("hogares_reportados") > 0,
            F.round(F.col("valor_total") / F.col("hogares_reportados"), 2),
        ).otherwise(F.lit(0.0)),
    )
    .orderBy(F.col("valor_total").desc())
    .cache()
)

por_programa = (
    df.groupBy("programa")
    .agg(
        F.count(F.lit(1)).alias("registros"),
        F.sum("hogares").alias("hogares_reportados"),
        F.sum("valor_asignado").alias("valor_total"),
    )
    .withColumn("participacion_valor_pct", porcentaje(F.col("valor_total"), total_valor))
    .withColumn(
        "participacion_hogares_pct",
        porcentaje(F.col("hogares_reportados"), total_hogares),
    )
    .withColumn(
        "valor_promedio_hogar",
        F.when(
            F.col("hogares_reportados") > 0,
            F.round(F.col("valor_total") / F.col("hogares_reportados"), 2),
        ).otherwise(F.lit(0.0)),
    )
    .orderBy(F.col("valor_total").desc())
    .cache()
)

print("\n=== TOP 10 DEPARTAMENTOS POR MONTO TOTAL ASIGNADO ===")
mostrar_tabla(
    por_departamento.select(
        "departamento",
        "hogares_reportados",
        "valor_total",
        "participacion_valor_pct",
        "participacion_hogares_pct",
        "valor_promedio_hogar",
    ),
    limite=10,
    dinero=("valor_total", "valor_promedio_hogar"),
    enteros=("hogares_reportados",),
    porcentajes=("participacion_valor_pct", "participacion_hogares_pct"),
)

print("\n=== TOP 15 MUNICIPIOS POR MONTO TOTAL ASIGNADO ===")
mostrar_tabla(
    por_municipio.select(
        "departamento",
        "municipio",
        "hogares_reportados",
        "valor_total",
        "participacion_valor_pct",
        "participacion_hogares_pct",
    ),
    limite=15,
    dinero=("valor_total",),
    enteros=("hogares_reportados",),
    porcentajes=("participacion_valor_pct", "participacion_hogares_pct"),
)

print("\n=== DISTRIBUCIÓN DE SUBSIDIOS POR PROGRAMA ===")
mostrar_tabla(
    por_programa.select(
        "programa",
        "hogares_reportados",
        "valor_total",
        "participacion_valor_pct",
        "participacion_hogares_pct",
        "valor_promedio_hogar",
        "registros",
    ),
    limite=50,
    dinero=("valor_total", "valor_promedio_hogar"),
    enteros=("hogares_reportados", "registros"),
    porcentajes=("participacion_valor_pct", "participacion_hogares_pct"),
)

print("\n=== EVOLUCIÓN ANUAL: HOGARES Y MONTO ASIGNADO ===")
evolucion_anual = (
    df.filter(F.col("anio_asignacion").isNotNull())
    .groupBy("anio_asignacion")
    .agg(
        F.count(F.lit(1)).alias("registros"),
        F.sum("hogares").alias("hogares_reportados"),
        F.sum("valor_asignado").alias("valor_total"),
    )
    .orderBy("anio_asignacion")
)
mostrar_tabla(
    evolucion_anual,
    limite=100,
    dinero=("valor_total",),
    enteros=("registros", "hogares_reportados"),
)

# Gráficas opcionales para evidenciar visualmente los resultados del batch.
# Solo se transfieren al driver los agregados resumidos (top 10), no el dataset completo.
print("\n=== GENERACIÓN DE GRÁFICAS ===")
try:
    import matplotlib
    matplotlib.use("Agg")  # Permite generar imágenes en entornos sin interfaz gráfica.
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter

    carpeta_graficas = os.path.join(os.getcwd(), "resultados_subsidios")
    os.makedirs(carpeta_graficas, exist_ok=True)

    def guardar_grafica_barras(filas, etiqueta, columna_valor, titulo, nombre_archivo,
                               formato_valor="dinero"):
        if not filas:
            print("No hay datos para generar: %s" % titulo)
            return

        etiquetas = [str(fila[etiqueta]) for fila in filas]
        valores = [float(fila[columna_valor] or 0) for fila in filas]
        fig, ax = plt.subplots(figsize=(12, max(5, len(etiquetas) * 0.45)))
        ax.barh(etiquetas[::-1], valores[::-1])
        ax.set_title(titulo)
        ax.set_xlabel("Monto asignado (COP)" if formato_valor == "dinero" else "Hogares reportados")
        ax.grid(axis="x", linestyle="--", alpha=0.35)
        ax.set_axisbelow(True)
        if formato_valor == "dinero":
            ax.xaxis.set_major_formatter(
                FuncFormatter(lambda valor, posicion: "$ {:,.0f}".format(valor))
            )
        fig.tight_layout()
        ruta = os.path.join(carpeta_graficas, nombre_archivo)
        fig.savefig(ruta, dpi=160, bbox_inches="tight")
        plt.close(fig)
        print("Gráfica guardada: %s" % ruta)

    filas_departamentos = (
        por_departamento.select("departamento", "valor_total").limit(10).collect()
    )
    filas_municipios = (
        por_municipio.withColumn(
            "municipio_etiqueta",
            F.concat_ws(" - ", F.col("municipio"), F.col("departamento")),
        )
        .select("municipio_etiqueta", "valor_total")
        .limit(10)
        .collect()
    )
    filas_programas_monto = (
        por_programa.select("programa", "valor_total").limit(10).collect()
    )
    filas_programas_hogares = (
        por_programa.orderBy(F.col("hogares_reportados").desc())
        .select("programa", "hogares_reportados")
        .limit(10)
        .collect()
    )

    guardar_grafica_barras(
        filas_departamentos, "departamento", "valor_total",
        "Top 10 departamentos por monto total asignado",
        "top_10_departamentos_valor.png",
    )
    guardar_grafica_barras(
        filas_municipios, "municipio_etiqueta", "valor_total",
        "Top 10 municipios por monto total asignado",
        "top_10_municipios_valor.png",
    )
    guardar_grafica_barras(
        filas_programas_monto, "programa", "valor_total",
        "Top 10 programas por monto total asignado",
        "top_10_programas_valor.png",
    )
    guardar_grafica_barras(
        filas_programas_hogares, "programa", "hogares_reportados",
        "Top 10 programas por hogares reportados",
        "top_10_programas_hogares.png",
        formato_valor="entero",
    )
    print("Las tablas completas siguen disponibles en la salida de consola.")
except ImportError:
    print(
        "No se generaron gráficas porque matplotlib no está instalado. "
        "El análisis y las tablas de Spark se ejecutaron de forma independiente."
    )
except Exception as error_graficas:
    print(
        "No fue posible generar una o más gráficas: %s. "
        "Las tablas del análisis siguen disponibles." % error_graficas
    )

if "estado_postulacion" in df.columns:
    print("\n=== REGISTROS POR ESTADO DE POSTULACIÓN ===")
    por_estado = (
        df.groupBy("estado_postulacion")
        .agg(
            F.count(F.lit(1)).alias("registros"),
            F.sum("hogares").alias("hogares_reportados"),
            F.sum("valor_asignado").alias("valor_total"),
        )
        .orderBy(F.col("registros").desc())
    )
    mostrar_tabla(
        por_estado,
        limite=50,
        dinero=("valor_total",),
        enteros=("registros", "hogares_reportados"),
    )

# ---------------------------------------------------------------------------
# 8. Valores extremos y registros individuales de mayor monto
# ---------------------------------------------------------------------------
print("\n=== TOP 10 REGISTROS INDIVIDUALES POR VALOR ASIGNADO ===")
mostrar_tabla(
    df.orderBy(F.col("valor_asignado").desc()).select(
        "departamento",
        "municipio",
        "programa",
        "anio_asignacion",
        "hogares",
        "valor_asignado",
        "valor_por_hogar",
    ),
    limite=10,
    dinero=("valor_asignado", "valor_por_hogar"),
    enteros=("hogares",),
)

print("\n=== TOP 10 REGISTROS INDIVIDUALES POR NÚMERO DE HOGARES ===")
mostrar_tabla(
    df.orderBy(F.col("hogares").desc()).select(
        "departamento",
        "municipio",
        "programa",
        "anio_asignacion",
        "hogares",
        "valor_asignado",
    ),
    limite=10,
    dinero=("valor_asignado",),
    enteros=("hogares",),
)

# Error relativo 0.0 = percentil exacto. Con 0.01 el estimado devolvia el maximo y la tabla salia vacia.
cuantiles = df.approxQuantile("valor_asignado", [0.99], 0.0)
if cuantiles:
    umbral_p99 = cuantiles[0]
    cantidad_sobre_p99 = df.filter(F.col("valor_asignado") > umbral_p99).count()
    print("\n=== VALORES EXTREMOS: REGISTROS POR ENCIMA DEL PERCENTIL 99 ===")
    print("Umbral P99 exacto: %s" % moneda_co(umbral_p99))
    print("Registros por encima del umbral: {:,}".format(cantidad_sobre_p99))
    mostrar_tabla(
        df.filter(F.col("valor_asignado") > umbral_p99)
        .select(
            "departamento",
            "municipio",
            "programa",
            "anio_asignacion",
            "hogares",
            "valor_asignado",
        )
        .orderBy(F.col("valor_asignado").desc()),
        limite=20,
        dinero=("valor_asignado",),
        enteros=("hogares",),
    )

# ---------------------------------------------------------------------------
# 9. Conclusiones automáticas del EDA: se calculan con los resultados reales
# ---------------------------------------------------------------------------
print("\n" + "=" * 78)
print("HALLAZGOS PRINCIPALES DEL ANÁLISIS")
print("=" * 78)

fila_departamento = por_departamento.first()
fila_municipio = por_municipio.first()
fila_programa = por_programa.first()

if fila_departamento:
    print(
        "1. Departamento con mayor monto acumulado: {} - {} ({}% del total).".format(
            fila_departamento["departamento"],
            moneda_co(fila_departamento["valor_total"]),
            fila_departamento["participacion_valor_pct"],
        )
    )
if fila_municipio:
    print(
        "2. Municipio con mayor monto acumulado: {}, {} - {} ({}% del total).".format(
            fila_municipio["municipio"],
            fila_municipio["departamento"],
            moneda_co(fila_municipio["valor_total"]),
            fila_municipio["participacion_valor_pct"],
        )
    )
if fila_programa:
    print(
        "3. Programa con mayor valor asignado: {} - {} ({}% del total).".format(
            fila_programa["programa"],
            moneda_co(fila_programa["valor_total"]),
            fila_programa["participacion_valor_pct"],
        )
    )
print(
    "4. El procesamiento conservó {:,} registros de {:,} iniciales ({:.2f}% de retención).".format(
        total_final,
        total_inicial,
        (total_final / total_inicial * 100) if total_inicial else 0.0,
    )
)
print(
    "5. La suma registrada es de {:,} hogares y un monto total de {}.".format(
        total_hogares, moneda_co(total_valor)
    )
)
print(
    "Interpretación: use las participaciones porcentuales para comparar la concentración "
    "territorial y por programa. Los rankings se calculan sobre la suma de los registros "
    "limpios, no sobre el monto de una sola fila."
)

# ---------------------------------------------------------------------------
# 10. Consulta adicional con Spark SQL
# ---------------------------------------------------------------------------
df.createOrReplaceTempView("subsidios")
print("\n=== SPARK SQL: TOP 10 MUNICIPIOS POR VALOR TOTAL ===")
resultado_sql = spark.sql(
    """
    SELECT departamento,
           municipio,
           SUM(hogares) AS hogares_reportados,
           ROUND(SUM(valor_asignado), 2) AS valor_total
    FROM subsidios
    GROUP BY departamento, municipio
    ORDER BY valor_total DESC
    LIMIT 10
    """
)
mostrar_tabla(
    resultado_sql,
    limite=10,
    dinero=("valor_total",),
    enteros=("hogares_reportados",),
)

# Liberar las tablas agregadas cacheadas antes de cerrar la sesión.
por_departamento.unpersist()
por_municipio.unpersist()
por_programa.unpersist()
df.unpersist()

# Mantiene abierta la sesión para revisar Jobs/Stages en http://IP:4040.
try:
    input("\nProceso terminado. Revisa la UI de Spark y presiona ENTER para cerrar...")
except EOFError:
    # Permite ejecutar el script en entornos sin consola interactiva.
    pass
finally:
    spark.stop()
