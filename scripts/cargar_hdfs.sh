#!/usr/bin/env bash
# Descarga el dataset "Subsidios de Vivienda Asignados" y lo carga en HDFS.
# Ejecutar con el usuario que administra Hadoop, con el clúster iniciado (start-all.sh).
set -euo pipefail

URL="https://www.datos.gov.co/api/views/h2yr-zfb2/rows.csv?accessType=DOWNLOAD"
ARCHIVO="json.csv"
DESTINO="/TareaSubsidios"

hdfs dfs -mkdir -p "$DESTINO"
wget -O "$ARCHIVO" "$URL"
head -c 300 "$ARCHIVO"; echo
hdfs dfs -put -f "$ARCHIVO" "$DESTINO"
hdfs dfs -ls "$DESTINO"
