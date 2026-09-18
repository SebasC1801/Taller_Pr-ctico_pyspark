# Taller 1 – ETL con PySpark (Online Retail Dataset)

Este proyecto descarga el dataset **Online Retail** de UCI con **Scrapy**,
y luego lo procesa con **PySpark** respondiendo las 10 preguntas del taller.
Cada respuesta queda guardada como un archivo `.csv` en la carpeta `resultados/`.

## Estructura del proyecto

```
taller_pyspark/
├── requirements.txt
├── scraper/
│   └── uci_retail_spider.py    # Descarga el .xlsx desde UCI
├── etl/
│   └── etl_taller1.py          # ETL completo con PySpark, responde las 10 preguntas
├── data/                       # Aquí caen el .xlsx descargado y su conversión a .csv
└── resultados/                 # Aquí se generan los 11 archivos CSV de salida
```

## Pasos para correrlo en PowerShell

### 1. Crear y activar un entorno virtual (recomendado)

```powershell
cd ruta\donde\descomprimiste\taller_pyspark
python -m venv venv
venv\Scripts\activate
```

### 2. Instalar dependencias

```powershell
python -m pip install -r requirements.txt
```

Asegúrate de tener Java instalado (ya lo tienes, JDK 17 con Temurin) porque
PySpark lo necesita para correr.

### 3. Descargar el dataset con Scrapy

```powershell
cd scraper
python -m scrapy runspider uci_retail_spider.py
cd ..
```

Esto descarga `Online Retail.xlsx` (~22.6 MB) directamente desde el
repositorio de UCI y lo guarda en `data\Online_Retail.xlsx`.

Si el sitio de UCI llegara a fallar o cambiar de URL, puedes descargar el
archivo manualmente desde:
https://archive.ics.uci.edu/dataset/352/online+retail
y colocarlo en `data\Online_Retail.xlsx`.

### 4. Correr el ETL con PySpark

```powershell
cd etl
python etl_taller1.py
cd ..
```

El script:
1. Convierte el `.xlsx` a `.csv` (una sola vez, usando pandas + openpyxl).
2. Carga el CSV en Spark.
3. Limpia los datos (quita filas sin cliente o sin descripción de producto).
4. Aplica `select`, `filter`, `orderBy`, `groupBy` + `agg`, `withColumn`,
   `join` y una función de ventana (`row_number`).
5. Responde las 10 preguntas del taller, mostrando los resultados en
   consola y guardando cada uno en `resultados/`.

## Archivos que se generan en `resultados/`

| Archivo | Contenido |
|---|---|
| `00_resumen_respuestas.csv` | Las 10 respuestas en un solo archivo |
| `01_total_facturas.csv` | Pregunta 1 |
| `02_clientes_unicos.csv` | Pregunta 2 |
| `03_ingreso_total.csv` | Pregunta 3 |
| `04_top10_productos_por_cantidad.csv` | Pregunta 4 |
| `05_top10_clientes_por_gasto.csv` | Pregunta 5 |
| `06_top5_paises_sin_uk.csv` | Pregunta 6 |
| `07_ticket_promedio.csv` | Pregunta 7 |
| `08_stats_productos_por_factura.csv` | Pregunta 8 |
| `09_ventas_por_mes.csv` | Pregunta 9 |
| `10_porcentaje_devoluciones.csv` | Pregunta 10 |

## Para el entregable final

Te falta armar, además de este código y los CSV:
- Un documento corto con las conclusiones (puedes basarte en lo que
  imprime la consola al correr `etl_taller1.py`).
- Subir todo a un repositorio de GitHub y poner el link en ese documento.

## Notas

- En Windows, PySpark falla con `df.write.csv()` por el error de
  `winutils.exe`/HADOOP_HOME. Por eso el script trae los resultados con
  `.toPandas()` y los guarda con `pandas.to_csv()`. Como solo se exportan
  resultados agregados (pocas filas), no se pierde nada.
- En este equipo hay políticas de Windows que bloquean `pip.exe` y
  `scrapy.exe` directos: siempre usa `python -m pip ...` y
  `python -m scrapy ...`.
- La limpieza del ETL aplica dos filtros, ambos validados en la salida:
  1. Quita filas sin `CustomerID` o sin `Description` (~135080 filas).
  2. Quita filas duplicadas exactas (5268 filas). Esto no cambia el número
     de facturas únicas (22190) pero sí reduce las sumas de dinero, porque
     antes esas filas se contaban dos veces.
- El dataset trae valores de `Quantity` negativos, que corresponden a
  devoluciones/cancelaciones (facturas que empiezan con "C"). Por eso
  el ingreso total (pregunta 3) ya viene neto, restando esas devoluciones.
- Estás usando **Python 3.14**, que es muy reciente. Si `pyspark` o
  `scrapy` te dan algún error raro de compatibilidad, prueba crear el
  entorno virtual con una versión más estable como 3.11 o 3.12
  (`py -3.12 -m venv venv`, si tienes esa versión instalada también).
