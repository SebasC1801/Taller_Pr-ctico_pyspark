"""
Spider de Scrapy para descargar el dataset "Online Retail" desde
el repositorio UCI Machine Learning.

Fuente: https://archive.ics.uci.edu/dataset/352/online+retail
Archivo: Online Retail.xlsx (~22.6 MB)

Cómo correrlo (desde PowerShell, parado en la carpeta scraper/):
    scrapy runspider uci_retail_spider.py

El archivo quedará guardado en: ../data/Online_Retail.xlsx
"""

import os
import scrapy


class OnlineRetailSpider(scrapy.Spider):
    name = "online_retail"

    # URL directa del archivo en el repositorio de UCI
    start_urls = [
        "https://archive.ics.uci.edu/ml/machine-learning-databases/00352/Online%20Retail.xlsx"
    ]

    custom_settings = {
        # Algunos servidores bloquean el user-agent por defecto de Scrapy
        "USER_AGENT": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0 Safari/537.36"
        ),
        "ROBOTSTXT_OBEY": False,
        "DOWNLOAD_TIMEOUT": 180,
    }

    def parse(self, response):
        # Carpeta de salida: taller_pyspark/data
        output_dir = os.path.join(os.path.dirname(os.getcwd()), "data")
        os.makedirs(output_dir, exist_ok=True)

        filepath = os.path.join(output_dir, "Online_Retail.xlsx")
        body = response.body
        size_mb = len(body) / (1024 * 1024)

        # Validación de la descarga.
        # 1) Un .xlsx real es un archivo ZIP: sus primeros bytes son "PK".
        #    Si el servidor devuelve una página de error en HTML, no es "PK".
        # 2) El dataset oficial pesa ~22.6 MB; un archivo mucho más pequeño
        #    indica una descarga incorrecta (página de error o archivo malo).
        if not body[:2] == b"PK":
            raise RuntimeError(
                "El archivo descargado no tiene firma de archivo .xlsx (PK). "
                "Posible página de error del servidor; revisa la URL de descarga."
            )
        if size_mb < 20:
            raise RuntimeError(
                f"El archivo descargado pesa {size_mb:.2f} MB, menos de lo esperado "
                "(~22.6 MB). La descarga parece incompleta."
            )

        with open(filepath, "wb") as f:
            f.write(body)
        self.log(f"Archivo descargado correctamente: {filepath} ({size_mb:.2f} MB)")
