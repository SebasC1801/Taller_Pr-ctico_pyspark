# Taller 1 – ETL con PySpark · Online Retail Dataset

**Proyecto:** Aplicación de un proceso **ETL con PySpark** sobre el dataset
**Online Retail** (UCI Machine Learning Repository) para responder las
10 preguntas del taller. Cada respuesta se exporta a un archivo CSV y el
proceso completo queda documentado y validado.

> **Entregables del taller**
> 1. Script de PySpark con todas las operaciones solicitadas → `etl/etl_taller1.py`
> 2. CSVs con los resultados → carpeta `resultados/`
> 3. Documento corto de conclusiones → `CONCLUSIONES.md`
> 4. Repositorio de GitHub → [https://github.com/SebasC1801/Taller_Pr-ctico_pyspark](https://github.com/SebasC1801/Taller_Pr-ctico_pyspark)

---

## 1. Contexto y objetivo

El taller pide aplicar un ETL real con PySpark usando operaciones clave de
Spark: **lectura de CSV, `select`, `filter`/`where`, `orderBy`, agregaciones
(`sum/avg/min/max/count`), `groupBy`+`agg`, `withColumn`, `join` y funciones
de ventana (`row_number`)**, y responder 10 preguntas de negocio exportando
cada resultado a CSV.

## 2. Dataset

| Atributo | Valor |
|---|---|
| **Nombre** | Online Retail |
| **Fuente** | UCI Machine Learning Repository |
| **URL** | https://archive.ics.uci.edu/dataset/352/online+retail |
| **Formato original** | `Online_Retail.xlsx` (~22.6 MB) |
| **Transacciones** | 541,909 filas |
| **Período** | Dic 2010 – Dic 2011 |
| **Campos** | `InvoiceNo`, `StockCode`, `Description`, `Quantity`, `InvoiceDate`, `UnitPrice`, `CustomerID`, `Country` |

El archivo se descarga automáticamente con un spider de **Scrapy**
(`scraper/uci_retail_spider.py`), que **valida la descarga** (firma `PK` de
archivo Excel y tamaño > 20 MB) antes de guardarlo.

## 3. Estructura del proyecto

```
taller_pyspark/
├── CONCLUSIONES.md              # Documento de conclusiones (entregable 3)
├── README.md                    # Este documento
├── requirements.txt             # pyspark, scrapy, pandas, openpyxl
├── scraper/
│   └── uci_retail_spider.py     # Descarga y valida el .xlsx desde UCI
├── etl/
│   └── etl_taller1.py           # ETL completo con PySpark (entregable 1)
├── data/
│   ├── Online_Retail.xlsx       # Dataset crudo (fuente)
│   └── Online_Retail.csv        # Conversión a CSV (se regenera)
└── resultados/                  # Los 11 CSV de salida (entregable 2)
```

## 4. Requisitos del entorno

- **Windows 11** con **PowerShell** y **Java 17 (Temurin)** (PySpark lo requiere).
- **Python 3.12–3.14** dentro de un entorno virtual (`.venv/`).
- En este equipo, la política **Control de Aplicaciones** de Windows bloquea
  `pip.exe` y `scrapy.exe` directos: **siempre usar `python -m pip` y
  `python -m scrapy`**.

## 5. Cómo ejecutar

### 5.1 Crear el entorno virtual e instalar dependencias

```powershell
cd C:\ruta\al\proyecto\taller_pyspark
python -m venv .venv
.venv\Scripts\activate
python -m pip install -r requirements.txt
```

### 5.2 Descargar el dataset (solo la primera vez)

```powershell
cd scraper
python -m scrapy runspider uci_retail_spider.py
cd ..
```

Descarga `Online_Retail.xlsx` a `data\`. Si UCI cayera, descárgalo
manualmente desde https://archive.ics.uci.edu/dataset/352/online+retail
y colócalo en `data\`.

### 5.3 Correr el ETL

```powershell
cd etl
python etl_taller1.py
cd ..
```

El script: (1) convierte el `.xlsx` a `.csv` con pandas, (2) carga el CSV en
Spark, (3) limpia y valida los datos, (4) aplica todas las operaciones de
Spark pedidas, y (5) responde las 10 preguntas mostrando resultados en
consola y guardando los 11 CSV en `resultados/`.

## 6. Limpieza y validación de datos

El proceso es **trazable**: cada decisión quedó impresa en la salida.

| Paso | Filas | Detalle |
|---|---|---|
| Filas iniciales | **541,909** | Archivo crudo |
| Tras quitar `CustomerID`/`Description` nulos | **406,829** | Se quitaron 135,080 |
| Tras quitar filas duplicadas exactas | **401,604** | Se quitaron 5,225 |
| Países distintos detectados | **37** | Sin errores de digitación |

Validaciones adicionales realizadas (auditoría completa):
- `InvoiceNo`: sin vacíos, sin espacios, sin duplicados por mayúsculas.
- `CustomerID`: sin ceros, sin negativos, sin valores corruptos.
- `Country`: nombres bien escritos; el Reino Unido aparece exactamente como
  `"United Kingdom"` (el filtro de la pregunta 6 lo excluye por ese nombre).
- Fechas: 100% legibles en formato ISO (`yyyy-MM-dd HH:mm:ss`).
- Se **documentan pero no se borran** filas con `UnitPrice == 0` (2,515),
  `UnitPrice < 0` (2) o descripciones `?`, por ser parte del dataset oficial.

## 7. Respuestas del taller

| # | Pregunta | Resultado |
|---|----------|-----------|
| 1 | Número total de facturas | **22,190** |
| 2 | Clientes únicos | **4,372** |
| 3 | Ingreso total (`Quantity * UnitPrice`) | **$8,278,519.42** |
| 4 | Producto más vendido en cantidad | **WORLD WAR 2 GLIDERS ASSTD DESIGNS** · 53,119 u |
| 5 | Cliente con mayor gasto | **14646** · $279,489.02 |
| 6 | Top 5 países fuera de Reino Unido | Netherlands ($284,661.54) · EIRE ($250,001.78) · Germany ($221,509.47) · France ($196,626.05) · Australia ($137,009.77) |
| 7 | Ticket promedio por factura | **$373.07** |
| 8 | Unidades por factura (mín / máx / prom) | **-80,995 / 80,995 / 220.50** |
| 9 | Mes con más ventas | **2011-11** · $1,126,815.07 |
| 10 | % de facturas con devoluciones (`Quantity < 0`) | **16.47%** (3,654 / 22,190) |

### Lectura rápida de resultados

- El promedio de **220.5 unidades por factura** está distorsionado por
  facturas de ajuste atípicas (mín. -80,995 y máx. 80,995).
- El **16.47%** de las facturas tiene devoluciones; el ingreso total (punto 3)
  ya es **neto** porque resta esas devoluciones.
- **Noviembre 2011** es el mes pico, consistente con la temporada navideña.
- Reino Unido domina, pero el primer mercado externo es Países Bajos y el
  segundo Irlanda (`EIRE`).

## 8. Importante: discrepancia en el número de facturas

El profesor mencionó en clase que tras limpiar nulos y duplicados quedarían
**"unas 2,200 facturas"**. Con el dataset oficial completo de UCI (descarga
verificada) el valor real es **22,190**. Se descartó que haya errores de
digitación en los datos y ninguna limpieza estándar (cancelaciones, precios
≤ 0, códigos no-producto, etc.) llega a ~2,200. Las hipótesis más probables:

1. El profesor trabajó con un **subconjunto diferente**, p. ej. solo facturas
   fuera de Reino Unido (**2,333**) o fuera de Reino Unido en 2011 (**2,197**).
2. Se perdió un cero al comunicar el número ("22 mil" ≈ 22,190 vs. "2,200").

**Decisión:** el análisis se entrega sobre el **dataset completo (22,190
facturas)**, que es la base correcta, reproducible y consistente con las 10
preguntas. La discrepancia queda documentada para aclararla con el profesor.

## 9. Archivos de salida en `resultados/`

| Archivo | Contenido |
|---|---|
| `00_resumen_respuestas.csv` | Las 10 respuestas consolidadas |
| `01_total_facturas.csv` | Pregunta 1 |
| `02_clientes_unicos.csv` | Pregunta 2 |
| `03_ingreso_total.csv` | Pregunta 3 |
| `04_top10_productos_por_cantidad.csv` | Pregunta 4 (top 10) |
| `05_top10_clientes_por_gasto.csv` | Pregunta 5 (top 10 con ranking `row_number`) |
| `06_top5_paises_sin_uk.csv` | Pregunta 6 (con `join`) |
| `07_ticket_promedio.csv` | Pregunta 7 |
| `08_stats_productos_por_factura.csv` | Pregunta 8 |
| `09_ventas_por_mes.csv` | Pregunta 9 |
| `10_porcentaje_devoluciones.csv` | Pregunta 10 |

## 10. Notas técnicas (Windows)

- **`df.write.csv()` no se usa**: en Windows PySpark falla por
  `winutils.exe`/HADOOP_HOME. Los resultados se exportan con `.toPandas()` +
  `pandas.to_csv()`. Seguro porque son datos agregados (pocas filas).
- Los logs de Spark muestran el warning de `winutils.exe` y el de *PyArrow no
  instalado*; **ambos son inofensivos**. Puedes instalar `pyarrow` con
  `python -m pip install pyarrow` para acelerar `toPandas()`.
- Con Python 3.14 algunos paquetes pueden dar warnings de compatibilidad; si
  aparecen errores, usa una versión más estable (`py -3.12 -m venv .venv`).