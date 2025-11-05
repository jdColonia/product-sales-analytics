# 📊 Pipeline de Análisis Exploratorio de Datos

Pipeline de análisis exploratorio de datos (EDA) desarrollado con Apache Spark para el análisis de ventas de productos.

## ✒️ Autores

> - Juan David Colonia Aldana - A00395956
> - Miguel Ángel Gonzalez Arango - A00395687

## 🎯 Objetivo

Realizar un análisis exploratorio completo de datasets de ventas, incluyendo:

### 1. Revisión Inicial del Dataset

- Estructura del dataset (registros, columnas, tipos de datos)
- Identificación de valores faltantes o nulos
- Detección de registros duplicados

### 2. Estadísticas Descriptivas

**Variables Numéricas:**

- Medidas de tendencia central: media, mediana, moda
- Medidas de dispersión: desviación estándar, rango
- Percentiles: Q1 (25%), Q2 (50%), Q3 (75%)
- Detección de valores atípicos (outliers) mediante método IQR
- **Nota:** Los IDs (product_id, customer_id, category_id, store_id) NO se analizan con estadísticas descriptivas ya que carecen de significado estadístico

**Variables Categóricas:**

- Frecuencias absolutas y relativas
- Distribución por categorías
- Análisis de cardinalidad

**Análisis de IDs:**

- Cardinalidad (valores únicos)
- Frecuencias de aparición (productos más vendidos, clientes más activos)
- Detección de nulos o inconsistencias

### 3. Análisis Temporal

- Ventas diarias, semanales y mensuales
- Picos de ventas por día de la semana
- Análisis de tendencias y estacionalidad
- Promedios móviles y detección de patrones

### 4. Análisis de Clientes

- Frecuencia de compra por cliente
- Tiempo promedio entre compras
- Segmentación RFM (Recency, Frequency, Monetary)
- Categorización de clientes (Champions, At Risk, Lost, etc.)

### 5. Análisis Avanzado de Productos

- Productos más vendidos globalmente y por tienda
- Market Basket Analysis (Análisis de Canasta de Mercado)
- Reglas de asociación usando FP-Growth
- Patrones de compra conjunta (productos que se compran juntos)

## 🏗️ Estructura del Proyecto

```
product-sales-analytics/
├── config/
│   └── spark_config.py             # Configuración de Spark
├── data/
│   ├── products/
│   │   ├── Categories.csv
│   │   └── ProductCategory.csv
│   └── transactions/
│       └── *_Tran.csv
├── src/
│   ├── __init__.py                 # Inicialización del paquete
│   ├── analyzers/                  # Analizadores especializados
│   │   ├── __init__.py
│   │   ├── temporal_analyzer.py    # Análisis temporal de ventas
│   │   ├── customer_analyzer.py    # Análisis de clientes y segmentación
│   │   └── product_analyzer.py     # Análisis de productos y reglas de asociación
│   ├── data_loader.py              # Carga y preparación de datos
│   ├── eda_analyzer.py             # Motor de análisis exploratorio
│   ├── visualizer.py               # Generación de gráficas
│   ├── pipeline.py                 # Orquestador del pipeline
│   └── utils.py                    # Utilidades comunes
├── output/                         # Resultados generados (PNG)
│   └── plots/                      # Gráficas generadas
├── main.py                         # Script principal de ejecución
└── requirements.txt                # Dependencias del proyecto
```

## ⚡ Instalación

### Requisitos

- Python 3.10
- Java (requerido por Apache Spark)

### Pasos

```bash
# Activar entorno virtual
.\venv\Scripts\activate  # Windows
source venv/bin/activate # Linux/Mac

# Instalar dependencias
pip install -r requirements.txt
```

## 🚀 Uso

### Ejecución del Pipeline

```bash
python main.py
```

El pipeline ejecutará automáticamente:

1. Análisis de categorías de productos
2. Análisis de relación productos-categorías
3. Análisis de transacciones
4. Análisis de transacciones detalladas por producto
5. Análisis temporal (ventas diarias, semanales, mensuales, tendencias)
6. Análisis de clientes (frecuencia, tiempo entre compras, segmentación RFM)
7. Análisis avanzado de productos (Market Basket Analysis con FP-Growth)
8. Generación automática de todas las visualizaciones

### Configuración

Para ajustar el tamaño de las muestras o configuración de Spark, editar:

- `config/spark_config.py` - Configuración de memoria y recursos de Spark
- `main.py` - Tamaños de muestra para análisis

## 📊 Datasets Analizados

| Dataset         | Descripción                         |
| --------------- | ----------------------------------- |
| Categories      | Catálogo de categorías de productos |
| ProductCategory | Relación productos-categorías       |
| Transactions    | Transacciones de ventas por tienda  |

## 📁 Resultados

Los resultados del análisis se muestran en consola durante la ejecución. Las visualizaciones se guardan automáticamente en:

### Visualizaciones (PNG)

Todas las gráficas se generan numeradas en orden de ejecución:

```
output/plots/
# EDA Básico (Paso 1-4)
├── 01_category_distribution_category_distribution.png
├── 02_products_per_category_top_category_name.png
├── 03_top_customers_transactions_top_customer_id.png
├── 04_temporal_trend_transactions_temporal_trend.png
├── 05_top_products_exploded_top_product_id.png
├── 06_temporal_trend_exploded_temporal_trend.png
# Análisis Temporal (Paso 5)
├── 07_weekly_sales.png
├── 08_monthly_sales.png
├── 09_day_of_week_sales.png
├── 10_daily_trend_temporal_trend.png
# Análisis de Clientes (Paso 6)
├── 11_customer_frequency_distribution.png
├── 12_customer_rfm_segments.png
# Análisis de Productos (Paso 7)
├── 13_top_products_detailed_top_product_id.png
└── 14_product_association_rules.png
```

**Tipos de gráficas generadas:**

**EDA Básico:**

- Distribución de productos por categoría (pie chart y barras)
- Top clientes con más transacciones
- Top productos más vendidos
- Tendencias temporales básicas

**Análisis Temporal:**

- Ventas semanales (gráfico de línea)
- Ventas mensuales (barras + línea de tendencia)
- Distribución por día de la semana (barras coloridas)
- Tendencia diaria con fechas en quincenas de pago (1, 15, 30)

**Análisis de Clientes:**

- Distribución de frecuencia de compra
- Segmentos RFM (Champions, Loyal, At Risk, Lost, etc.)

**Análisis de Productos:**

- Top 20 productos más vendidos
- Reglas de asociación (scatter plot: Confidence vs Lift)

## 🔧 Arquitectura

### Módulos Principales

**data_loader.py**

- Carga de datos desde archivos CSV
- Transformación y preparación de datos
- Gestión de múltiples fuentes de datos
- Explosión de datos de transacciones

**eda_analyzer.py**

- Análisis de estructura de datasets
- Cálculo de estadísticas descriptivas
- Detección de outliers y anomalías
- Generación de reportes básicos

**temporal_analyzer.py**

- Análisis de ventas diarias, semanales y mensuales
- Detección de picos y patrones por día de semana
- Cálculo de tendencias y promedios móviles
- Análisis de estacionalidad

**customer_analyzer.py**

- Análisis de frecuencia de compra por cliente
- Cálculo de tiempo entre compras
- Segmentación RFM (Recency, Frequency, Monetary)
- Categorización de clientes

**product_analyzer.py**

- Análisis de productos más vendidos
- Market Basket Analysis con FP-Growth
- Generación de reglas de asociación
- Análisis de combinaciones de productos

**pipeline.py**

- Orquestación del flujo completo de análisis
- Gestión de recursos de Spark
- Control de ejecución y logging
- Coordinación entre módulos

**visualizer.py**

- Generación automática de gráficas
- Visualización de frecuencias categóricas
- Gráficos de tendencias temporales
- Visualización de segmentos de clientes
- Gráficos de reglas de asociación
- Exportación de imágenes en alta resolución

**utils.py**

- Funciones auxiliares
- Formateo de salida
- Utilidades comunes

## 🛠️ Tecnologías

- Apache Spark 3.5.0
- PySpark
- Python 3.8+
- Pandas 2.0.3
- NumPy 1.24.3
- Matplotlib 3.7.2
- Seaborn 0.12.2
