# Decisiones de limpieza y transformación

Documento de metodología del procesamiento batch (`src/subsidios.py`). Cada decisión indica **qué se hace, por qué y qué efecto tuvo** sobre el conjunto de datos *Subsidios de Vivienda Asignados* (datos.gov.co, `h2yr-zfb2`).

Los efectos medidos corresponden a una ejecución sobre el archivo de **87.095 registros** y 13 columnas.

## Resumen del flujo

| Etapa | Registros |
|---|---|
| Leídos del archivo | 87.095 |
| Eliminados por limpieza (duplicados exactos) | 30.375 (34,9 %) |
| **Analizados** | **56.720 (65,1 % conservado)** |

## Decisiones

| # | Etapa | Decisión | Justificación | Efecto medido |
|---|---|---|---|---|
| 1 | Lectura | Se detecta si el archivo es JSON (API de Socrata) o CSV y se normalizan los nombres de columna (sin tildes ni espacios) | La API devuelve nombres como `a_o_de_asignaci_n` y `estado_de_postulaci_n`; el CSV de descarga trae `AÑO DE ASIGNACIÓN`. El mismo código funciona con ambos | Falla controlada si falta una columna obligatoria |
| 2 | Selección de columnas | Se conservan 9 de 13 columnas: las 7 del análisis más los códigos DIVIPOLA | Las columnas de sistema de Socrata (`:id`, `:version`, `:created_at`, `:updated_at`) no aportan al análisis | 13 → 9 columnas |
| 3 | Texto | `trim` y mayúsculas en departamento, municipio, programa y estado | Evita que una misma categoría quede repartida al agrupar (p. ej. «Bolsa Desplazados» y «BOLSA DESPLAZADOS») | 0 espacios sobrantes y 0 categorías repetidas en esta carga; se mantiene como salvaguarda |
| 4 | Tipos | `anio_asignacion` a entero, `hogares` a entero largo, `valor_asignado` a decimal | El JSON de la API entrega todos los campos como texto; sin convertirlos no se puede sumar ni ordenar | 0 valores no convertibles |
| 5 | Nulos | `dropna` sobre departamento, municipio, programa, hogares y valor | Sin ubicación, programa, hogares o valor el registro no se puede ubicar ni cuantificar | 0 filas eliminadas |
| 6 | Duplicados | `dropDuplicates()`: se eliminan filas idénticas en todos los campos | Evita contar dos veces un mismo registro | **30.375 filas eliminadas (34,9 %)** |
| 7 | Columna derivada | `valor_por_hogar = valor_asignado / hogares` (nulo si `hogares` = 0) | Permite comparar programas y territorios sin depender del tamaño del proyecto; evita dividir por cero | 47 filas con `hogares` = 0 quedan sin `valor_por_hogar` |
| 8 | Valores extremos | Los registros sobre el percentil 99 de `valor_asignado` se listan, **no se eliminan** | Corresponden a proyectos grandes legítimos; se muestran para revisión. El percentil se calcula exacto (`approxQuantile` con error 0,0) | P99 ≈ $4.498 millones; 567 registros por encima en la prueba local |

## Análisis de sensibilidad de la decisión 6 (duplicados)

La eliminación de duplicados exactos es la decisión con mayor impacto, por eso se midió cuánto cambia el análisis:

| Métrica | Efecto de eliminar duplicados |
|---|---|
| Registros | −34,9 % |
| Hogares reportados | −6,7 % |
| Valor asignado | −7,0 % |

- Las filas eliminadas son pequeñas: 2,2 hogares en promedio, 61 % con un solo hogar y 84 % pertenecen a *Mi Casa Ya*. Son subsidios de monto fijo, para los que es esperable que varios registros coincidan en municipio, programa, año, estado, hogares y valor.
- El archivo de la API trae un identificador único por fila (`:id`); el script lo descarta al seleccionar columnas, por eso esas filas se consideran duplicadas. Si se conservara el identificador, no se eliminarían.
- Los rankings principales se mantienen (departamento, municipio y programa líderes). Del cuarto departamento en adelante el orden cambia ligeramente.
- **Conclusión:** las cifras absolutas deben leerse como una cota inferior (cerca de 7 % por debajo si los duplicados fueran registros legítimos); la lectura relativa (participaciones, rankings) es robusta a esta decisión.

## Limitaciones que se declaran en el informe

- **Hogares reportados no son hogares únicos:** es la suma de la columna `hogares` de cada registro.
- **Estados del subsidio:** los montos incluyen subsidios que luego fueron vencidos, renunciados o revocados (≈ 4,3 % del valor y 8,7 % de los hogares en la prueba local); no se filtran por estado.
- **Año en curso incompleto:** 2026 solo cubre una parte del año.
- **Streaming simulado:** el productor reutiliza registros históricos con una fecha de evento actual; no son subsidios nuevos.
