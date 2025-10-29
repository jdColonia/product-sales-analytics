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

## 🏗️ Estructura del Proyecto

```
product-sales-analytics/
├── config/
│   └── spark_config.py          # Configuración de Spark
├── data/
│   ├── products/
│   │   ├── Categories.csv
│   │   └── ProductCategory.csv
│   └── transactions/
│       └── *_Tran.csv
├── src/
│   ├── __init__.py              # Inicialización del paquete
│   ├── data_loader.py           # Carga y preparación de datos
│   ├── eda_analyzer.py          # Motor de análisis exploratorio
│   ├── visualizer.py            # Generación de gráficas
│   ├── pipeline.py              # Orquestador del pipeline
│   └── utils.py                 # Utilidades comunes
├── output/                     # Resultados generados (JSON + PNG)
│   └── plots/                   # Gráficas generadas
├── main.py                      # Script principal de ejecución
└── requirements.txt             # Dependencias del proyecto
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
4. Análisis de transacciones detalladas
5. Generación automática de todas las visualizaciones

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

```
output/plots/
├── product_categories_category_distribution.png
├── products_per_category_top_category_name.png
├── transactions_top_customer_id.png
├── transactions_temporal_trend.png
├── transactions_exploded_top_product_id.png
├── transactions_exploded_temporal_trend.png
└── README.md
```

**Tipos de gráficas generadas:**

- Distribución de productos por categoría (pie chart y barras)
- Top clientes con más transacciones (barras horizontales)
- Top productos más vendidos (barras horizontales)
- Tendencias temporales de transacciones (gráficos de línea)

## 🔧 Arquitectura

### Módulos Principales

**data_loader.py**

- Carga de datos desde archivos CSV
- Transformación y preparación de datos
- Gestión de múltiples fuentes de datos

**eda_analyzer.py**

- Análisis de estructura de datasets
- Cálculo de estadísticas descriptivas
- Detección de outliers y anomalías
- Generación de reportes

**pipeline.py**

- Orquestación del flujo de análisis
- Gestión de recursos de Spark
- Control de ejecución y logging

**visualizer.py**

- Generación automática de gráficas
- Visualización de frecuencias categóricas
- Gráficos de tendencias temporales
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
