# Taller 1 – ETL con PySpark: Conclusiones

**Dataset:** Online Retail (UCI Machine Learning Repository) ·
**Alcance:** 541,909 transacciones · **Herramienta:** PySpark + pandas + Scrapy

---

## 1. Validación y limpieza aplicada

El proceso de ETL validado ejecuta la siguiente limpieza, y **cada paso queda
trazado en la salida** del script:

| Paso | Filas |
|---|---|
| Filas iniciales (archivo crudo) | 541,909 |
| Tras quitar `CustomerID` o `Description` nulos | 406,829 (se quitaron 135,080) |
| Tras quitar filas duplicadas exactas | 401,604 (se quitaron 5,225) |
| Países distintos detectados | 37 |

Se verificó la integridad de los campos: no hay `InvoiceNo` vacíos ni con
espacios, no hay `CustomerID` en cero/negativo, no hay errores de digitación
en los nombres de países (el Reino Unido aparece exactamente como
`"United Kingdom"`), y la fecha se lee 100% en formato ISO. Las filas con
`UnitPrice == 0` (2,515) o negativo (2) y las descripciones con `?` se
**dejaron tal cual** por pertenecer al dataset oficial; solo se documentan.

Nota de implementación (Windows): en lugar de `df.write.csv()` —que falla por
`winutils.exe`/HADOOP— los resultados se exportan con `.toPandas()` +
`pandas.to_csv()`, lo cual es seguro porque son resultados agregados.

---

## 2. Respuestas del análisis

| # | Pregunta | Resultado |
|---|---|---|
| 1 | Número total de facturas | **22,190** |
| 2 | Clientes únicos | **4,372** |
| 3 | Ingreso total (`Quantity * UnitPrice`) | **$8,278,519.42** |
| 4 | Producto más vendido en cantidad | **WORLD WAR 2 GLIDERS ASSTD DESIGNS** (53,119 unidades) |
| 5 | Cliente con mayor gasto | **14646** ($279,489.02) |
| 6 | Top 5 países fuera de Reino Unido | Netherlands ($284,661.54), EIRE ($250,001.78), Germany ($221,509.47), France ($196,626.05), Australia ($137,009.77) |
| 7 | Ticket promedio por factura | **$373.07** |
| 8 | Unidades por factura (mín / máx / prom) | **-80,995 / 80,995 / 220.50** |
| 9 | Mes con más ventas | **2011-11** ($1,126,815.07) |
| 10 | % de facturas con devoluciones (`Quantity < 0`) | **16.47%** (3,654 / 22,190) |

### Lectura de los resultados

- El promedio de **220.5 unidades por factura** está distorsionado por facturas
  atípicas (mín. -80,995 y máx. 80,995), que son ajustes/devoluciones masivos.
- El **16.47%** de las facturas involucra devoluciones; por eso el ingreso total
  del punto 3 **ya es un valor neto** (reste devoluciones).
- Noviembre 2011 concentra la mayor venta, consistente con temporada alta de
  Navidad en retail.
- Reino Unido domina el volumen, pero los 5 mayores mercados externos son
  Países Bajos, Irlanda (EIRE), Alemania, Francia y Australia.
- La columna de ranking de clientes (pregunta 5) se calculó con la función de
  ventana `row_number()`, tal como pide el taller.

---

## 3. Nota pendiente: discrepancia del número de facturas con el profesor

El profesor mencionó en clase que tras limpiar nulos y duplicados deberían
quedar "unas 2,200 facturas". Con el **dataset oficial completo de UCI
(541,909 filas, descarga verificada)**, el conteo real tras esa limpieza es
**22,190 facturas** (dato confirmado en este diagnóstico).

Se descartaron explicaciones por errores de datos: no hay `InvoiceNo`
duplicados por espacios/case, no hay países mal escritos y ninguna limpieza
estándar (quitar cancelaciones, `UnitPrice <= 0`, códigos no-producto, etc.)
reduce el conteo a ~2,200. Las opciones más probables son:

1. El profesor usó un **subconjunto diferente al dataset completo** (por
   ejemplo, solo facturas fuera de Reino Unido = **2,333**; o fuera de Reino
   Unido en 2011 = **2,197**, que coincide casi exacto con 2,200).
2. Se perdió un cero en la comunicación ("22 mil" ≈ 22,190 vs. "2,200").

**Acción tomada:** este trabajo mantiene el análisis sobre el dataset completo
(22,190 facturas), que es la base correcta y reproducible, y queda documentada
aquí la discrepancia para aclararla directamente con el profesor.

---

## 4. Reproducibilidad

```powershell
cd scraper
python -m scrapy runspider uci_retail_spider.py   # descarga el .xlsx

cd ..\etl
python etl_taller1.py                             # genera los 11 CSV en resultados\
```

---

## 5. Enlace al repositorio

**GitHub:** https://github.com/SebasC1801/Taller_Pr-ctico_pyspark