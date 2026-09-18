"""
Taller 1 - ETL con PySpark (Online Retail Dataset)

Este script:
  1. Convierte el .xlsx descargado por el spider a .csv (usando pandas,
     porque leer Excel nativo en Spark requiere un conector aparte).
  2. Carga el CSV en Spark y aplica limpieza básica con validación:
       - filas sin CustomerID o sin Description => se eliminan (~135080)
       - filas duplicadas exactas               => se eliminan (~5268)
     y se imprime cuántas filas se quitaron en cada paso para que el
     proceso quede trazable.
  3. Aplica select, filter, orderBy, agregaciones, groupBy+agg,
     withColumn, join y funciones de ventana.
  4. Responde las 10 preguntas del taller y guarda cada respuesta en
     su propio CSV dentro de la carpeta resultados/.

Notas de calidad del dataset original (se dejan tal cual, solo se documentan):
  - UnitPrice == 0 en 2515 filas y UnitPrice negativo en 2 filas.
  - Fechas 100% legibles: pandas escribe el CSV en formato ISO
    (yyyy-MM-dd HH:mm:ss) y Spark lo infiere como timestamp, por lo que
    la columna InvoiceDate se usa directamente en los cálculos.
  - El país de Reino Unido aparece escrito "United Kingdom" y el filtro
    de la pregunta 6 lo excluye por ese nombre exacto.

Cómo correrlo (desde PowerShell, parado en la carpeta etl/):
    python etl_taller1.py
"""

import os

import pandas as pd
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# 0. Rutas
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
RESULTS_DIR = os.path.join(BASE_DIR, "resultados")

XLSX_PATH = os.path.join(DATA_DIR, "Online_Retail.xlsx")
CSV_PATH = os.path.join(DATA_DIR, "Online_Retail.csv")

os.makedirs(RESULTS_DIR, exist_ok=True)


def convertir_xlsx_a_csv():
    """Convierte el Excel descargado a CSV una sola vez (si no existe ya)."""
    if os.path.exists(CSV_PATH):
        print(f"CSV ya existe, se usa el que esta en: {CSV_PATH}")
        return

    if not os.path.exists(XLSX_PATH):
        raise FileNotFoundError(
            f"No se encontró {XLSX_PATH}. Corre primero el spider de Scrapy "
            "(scraper/uci_retail_spider.py) para descargar el dataset."
        )

    print("Convirtiendo Online_Retail.xlsx a CSV (puede tardar un poco)...")
    df = pd.read_excel(XLSX_PATH, engine="openpyxl")
    df.to_csv(CSV_PATH, index=False, encoding="utf-8")
    print(f"CSV generado en: {CSV_PATH}")


def guardar_csv(df, nombre_archivo):
    """
    Guarda un DataFrame de Spark como un único CSV con el nombre indicado
    dentro de resultados/. En vez de usar df.write.csv() (que en Windows
    requiere winutils.exe de Hadoop y suele fallar), traemos el resultado
    a pandas con toPandas() y lo guardamos directamente. Como aquí solo
    guardamos resultados ya agregados (pocas filas), esto es seguro y
    evita esa dependencia de Hadoop por completo.
    """
    destino = os.path.join(RESULTS_DIR, nombre_archivo)
    df.toPandas().to_csv(destino, index=False, encoding="utf-8")
    print(f"  -> Guardado: resultados/{nombre_archivo}")


def main():
    convertir_xlsx_a_csv()

    spark = (
        SparkSession.builder.appName("Taller1_ETL_OnlineRetail")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # -----------------------------------------------------------------
    # 1. Lectura de datos
    # -----------------------------------------------------------------
    df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(CSV_PATH)

    print("Esquema original:")
    df.printSchema()

    # -----------------------------------------------------------------
    # 2. Limpieza y selección de columnas
    # -----------------------------------------------------------------
    df = df.select(
        "InvoiceNo", "StockCode", "Description", "Quantity",
        "InvoiceDate", "UnitPrice", "CustomerID", "Country",
    )

    filas_iniciales = df.count()

    # Quitamos filas sin cliente o sin descripción de producto
    df = df.filter(F.col("CustomerID").isNotNull() & F.col("Description").isNotNull())
    filas_sin_nulos = df.count()

    # Quitamos duplicados exactos: no cambian el número de facturas únicas,
    # pero sí inflan las sumas de Quantity/UnitPrice si no se quitan
    # (el dataset original trae 5268 filas duplicadas)
    df = df.dropDuplicates()
    filas_finales = df.count()

    # Validación de la limpieza: mostramos cuántas filas se quitaron en cada paso.
    # Nota: las filas con UnitPrice == 0 (2515) o negativo (2) se dejan tal cual,
    # porque forman parte del dataset oficial y quitarlas cambiaría las respuestas.
    paises_distintos = df.select("Country").distinct().count()
    fechas_nulas = df.filter(F.col("InvoiceDate").isNull()).count()
    print("Validacion de la limpieza:")
    print(f"  Filas iniciales                    : {filas_iniciales}")
    print(f"  Tras quitar nulos (Cust/Descr)     : {filas_sin_nulos} (se quitaron {filas_iniciales - filas_sin_nulos})")
    print(f"  Tras quitar duplicados exactos     : {filas_finales} (se quitaron {filas_sin_nulos - filas_finales})")
    print(f"  Paises distintos                   : {paises_distintos}")
    print(f"  Filas con InvoiceDate nula         : {fechas_nulas}")

    # La columna InvoiceDate llega del CSV ya como timestamp: pandas lo escribe
    # en formato ISO (yyyy-MM-dd HH:mm:ss) y Spark lo infiere automáticamente,
    # así que no hace falta volver a parsearla.

    # -----------------------------------------------------------------
    # 3. Columna derivada: TotalPrice = Quantity * UnitPrice
    # -----------------------------------------------------------------
    df = df.withColumn("TotalPrice", F.round(F.col("Quantity") * F.col("UnitPrice"), 2))

    df.cache()

    respuestas = []  # (pregunta, valor) para el resumen final

    # ===================================================================
    # Pregunta 1: número total de facturas
    # ===================================================================
    total_facturas = df.select("InvoiceNo").distinct().count()
    print(f"\n1) Total de facturas: {total_facturas}")
    respuestas.append(("1. Numero total de facturas", total_facturas))
    guardar_csv(
        spark.createDataFrame([(total_facturas,)], ["total_facturas"]),
        "01_total_facturas.csv",
    )

    # ===================================================================
    # Pregunta 2: número de clientes únicos
    # ===================================================================
    clientes_unicos = df.select("CustomerID").distinct().count()
    print(f"2) Clientes unicos: {clientes_unicos}")
    respuestas.append(("2. Clientes unicos", clientes_unicos))
    guardar_csv(
        spark.createDataFrame([(clientes_unicos,)], ["clientes_unicos"]),
        "02_clientes_unicos.csv",
    )

    # ===================================================================
    # Pregunta 3: ingreso total (Quantity * UnitPrice)
    # ===================================================================
    ingreso_total = df.agg(F.round(F.sum("TotalPrice"), 2).alias("ingreso_total")).first()["ingreso_total"]
    print(f"3) Ingreso total: {ingreso_total}")
    respuestas.append(("3. Ingreso total", ingreso_total))
    guardar_csv(
        spark.createDataFrame([(ingreso_total,)], ["ingreso_total"]),
        "03_ingreso_total.csv",
    )

    # ===================================================================
    # Pregunta 4: producto más vendido en cantidad
    # ===================================================================
    producto_mas_vendido = (
        df.groupBy("Description")
        .agg(F.sum("Quantity").alias("cantidad_total"))
        .orderBy(F.desc("cantidad_total"))
    )
    guardar_csv(producto_mas_vendido.limit(10), "04_top10_productos_por_cantidad.csv")
    top_producto = producto_mas_vendido.first()
    print(f"4) Producto mas vendido: {top_producto['Description']} ({top_producto['cantidad_total']} unidades)")

    # ===================================================================
    # Pregunta 5: cliente con mayor volumen de compra en dinero
    # (se usa una función de ventana -row_number()- para calcular el
    # ranking, como pide el taller, dentro del mismo archivo de salida)
    # ===================================================================
    gasto_por_cliente = (
        df.groupBy("CustomerID")
        .agg(F.round(F.sum("TotalPrice"), 2).alias("total_gastado"))
    )
    ventana_clientes = Window.orderBy(F.desc("total_gastado"))
    gasto_por_cliente = gasto_por_cliente.withColumn(
        "ranking", F.row_number().over(ventana_clientes)
    ).orderBy("ranking")

    guardar_csv(gasto_por_cliente.limit(10), "05_top10_clientes_por_gasto.csv")
    top_cliente = gasto_por_cliente.first()
    print(f"5) Cliente que mas gasto: {top_cliente['CustomerID']} (${top_cliente['total_gastado']})")

    # ===================================================================
    # Pregunta 6: top 5 países que más compran fuera de Reino Unido
    # (se usa join() para combinar el gasto total con el número de
    # facturas por país, como pide el taller, en el mismo archivo)
    # ===================================================================
    gasto_por_pais = (
        df.filter(F.col("Country") != "United Kingdom")
        .groupBy("Country")
        .agg(F.round(F.sum("TotalPrice"), 2).alias("total_comprado"))
    )
    facturas_por_pais = (
        df.filter(F.col("Country") != "United Kingdom")
        .groupBy("Country")
        .agg(F.countDistinct("InvoiceNo").alias("num_facturas"))
    )
    top_paises = (
        gasto_por_pais.join(facturas_por_pais, on="Country", how="inner")
        .orderBy(F.desc("total_comprado"))
        .limit(5)
    )
    guardar_csv(top_paises, "06_top5_paises_sin_uk.csv")
    print("6) Top 5 paises (sin UK):")
    top_paises.show(truncate=False)

    # ===================================================================
    # Pregunta 7: ticket promedio por factura
    # ===================================================================
    total_por_factura = df.groupBy("InvoiceNo").agg(F.sum("TotalPrice").alias("total_factura"))
    ticket_promedio = total_por_factura.agg(F.round(F.avg("total_factura"), 2).alias("ticket_promedio")).first()["ticket_promedio"]
    print(f"7) Ticket promedio por factura: {ticket_promedio}")
    guardar_csv(
        spark.createDataFrame([(ticket_promedio,)], ["ticket_promedio"]),
        "07_ticket_promedio.csv",
    )

    # ===================================================================
    # Pregunta 8: min, max y promedio de productos (unidades) por factura
    # ===================================================================
    productos_por_factura = df.groupBy("InvoiceNo").agg(F.sum("Quantity").alias("unidades_por_factura"))
    stats_factura = productos_por_factura.agg(
        F.min("unidades_por_factura").alias("minimo"),
        F.max("unidades_por_factura").alias("maximo"),
        F.round(F.avg("unidades_por_factura"), 2).alias("promedio"),
    )
    guardar_csv(stats_factura, "08_stats_productos_por_factura.csv")
    stats_row = stats_factura.first()
    print(f"8) Min: {stats_row['minimo']}, Max: {stats_row['maximo']}, Promedio: {stats_row['promedio']}")

    # ===================================================================
    # Pregunta 9: mes del año con más ventas
    # ===================================================================
    ventas_por_mes = (
        df.withColumn("Mes", F.date_format("InvoiceDate", "yyyy-MM"))
        .groupBy("Mes")
        .agg(F.round(F.sum("TotalPrice"), 2).alias("ventas_totales"))
        .orderBy(F.desc("ventas_totales"))
    )
    guardar_csv(ventas_por_mes, "09_ventas_por_mes.csv")
    mes_top = ventas_por_mes.first()
    print(f"9) Mes con mas ventas: {mes_top['Mes']} (${mes_top['ventas_totales']})")

    # ===================================================================
    # Pregunta 10: % de facturas con devoluciones (Quantity negativo)
    # ===================================================================
    facturas_con_devolucion = df.filter(F.col("Quantity") < 0).select("InvoiceNo").distinct().count()
    porcentaje_devoluciones = round((facturas_con_devolucion / total_facturas) * 100, 2)
    print(f"10) % de facturas con devoluciones: {porcentaje_devoluciones}%")
    guardar_csv(
        spark.createDataFrame(
            [(facturas_con_devolucion, total_facturas, porcentaje_devoluciones)],
            ["facturas_con_devolucion", "total_facturas", "porcentaje"],
        ),
        "10_porcentaje_devoluciones.csv",
    )

    # ===================================================================
    # Resumen final con las 10 respuestas en un solo CSV
    # ===================================================================
    resumen_df = pd.DataFrame(respuestas, columns=["pregunta", "valor"])
    resumen_path = os.path.join(RESULTS_DIR, "00_resumen_respuestas.csv")
    resumen_df.to_csv(resumen_path, index=False, encoding="utf-8")
    print(f"\nResumen guardado en: {resumen_path}")

    spark.stop()
    print("\nProceso ETL finalizado. Revisa la carpeta 'resultados/'.")


if __name__ == "__main__":
    main()