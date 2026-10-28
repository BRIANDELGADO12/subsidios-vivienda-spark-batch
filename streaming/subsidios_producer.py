# -*- coding: utf-8 -*-
# Productor Kafka: simula la llegada en tiempo real de subsidios de vivienda
# tomando registros reales del dataset (datos.gov.co, h2yr-zfb2).
import csv
import json
import random
import re
import sys
import time
import unicodedata
from datetime import datetime

from kafka import KafkaProducer

TOPIC = 'subsidios_topic'
BOOTSTRAP = 'localhost:9092'
DATA_FILE = sys.argv[1] if len(sys.argv) > 1 else 'subsidios_real.json'
INTERVALO = 1.0   # segundos entre eventos


def normalizar(nombre):
    n = unicodedata.normalize('NFKD', str(nombre)).encode('ascii', 'ignore').decode().lower()
    return re.sub(r'[^a-z0-9]+', '_', n).strip('_')


def buscar(reg, condicion):
    for k, v in reg.items():
        if condicion(k):
            return v
    return None


def cargar_registros(ruta):
    """Lee el archivo (JSON o CSV) y devuelve solo los campos necesarios."""
    with open(ruta, encoding='utf-8-sig') as f:
        inicio = f.read(1)
        f.seek(0)
        if inicio in ('[', '{'):
            crudos = json.load(f)
        else:
            crudos = list(csv.DictReader(f))

    registros = []
    for r in crudos:
        r = {normalizar(k): v for k, v in r.items()}
        try:
            hogares = int(float(buscar(r, lambda k: k == 'hogares')))
            valor = float(buscar(r, lambda k: k == 'valor_asignado'))
        except (TypeError, ValueError):
            continue
        departamento = buscar(r, lambda k: k == 'departamento')
        municipio = buscar(r, lambda k: k == 'municipio')
        programa = buscar(r, lambda k: k == 'programa')
        if not (departamento and municipio and programa):
            continue
        anio = buscar(r, lambda k: 'asignaci' in k and k.startswith(('a_o', 'ano', 'anio')))
        estado = buscar(r, lambda k: k.startswith('estado'))
        try:
            anio = int(float(anio))
        except (TypeError, ValueError):
            anio = None
        registros.append({
            'departamento': str(departamento).strip().upper(),
            'municipio': str(municipio).strip().upper(),
            'programa': str(programa).strip().upper(),
            'anio_asignacion': anio,
            'estado_postulacion': str(estado).strip().upper() if estado else None,
            'hogares': hogares,
            'valor_asignado': valor,
        })
    return registros


def main():
    registros = cargar_registros(DATA_FILE)
    if not registros:
        raise SystemExit('No se pudieron leer registros de %s' % DATA_FILE)
    print('Registros reales cargados: %d' % len(registros))

    producer = KafkaProducer(
        bootstrap_servers=[BOOTSTRAP],
        value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8'))

    try:
        while True:
            evento = dict(random.choice(registros))
            evento['fecha_evento'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            producer.send(TOPIC, value=evento)
            print('Sent: %s' % evento)
            time.sleep(INTERVALO)
    except KeyboardInterrupt:
        print('\nDeteniendo productor...')
    finally:
        producer.flush()
        producer.close()


if __name__ == '__main__':
    main()
