"""
Pipeline principal de análisis exploratorio de datos.

Este módulo orquesta el flujo completo de análisis EDA,
gestionando la carga de datos, ejecución de análisis y
generación de reportes.
"""

import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config.spark_config import create_spark_session, stop_spark_session
from src.data_loader import DataLoader
from src.eda_analyzer import EDAAnalyzer
from src.visualizer import DataVisualizer
from datetime import datetime


class SalesAnalyticsPipeline:
    """Pipeline principal para análisis de datos de ventas."""

    def __init__(self, app_name: str = "SalesAnalytics-EDA"):
        """
        Inicializa el pipeline.

        Args:
            app_name: Nombre de la aplicación Spark
        """
        self.app_name = app_name
        self.spark = None
        self.data_loader = None
        self.eda_analyzer = None
        self.visualizer = None
        self.start_time = None

    def initialize(self):
        """Inicializa los componentes del pipeline."""
        print(f"\n{'='*80}")
        print(f"🚀 INICIANDO PIPELINE DE ANÁLISIS EXPLORATORIO DE DATOS")
        print(f"{'='*80}\n")

        self.start_time = datetime.now()

        print("🔧 Inicializando sesión de Spark...")
        self.spark = create_spark_session(self.app_name)
        print("✅ Sesión de Spark inicializada correctamente\n")

        self.data_loader = DataLoader(self.spark)
        self.eda_analyzer = EDAAnalyzer(self.spark)
        self.visualizer = DataVisualizer()

    def run_categories_analysis(self):
        """Ejecuta análisis del dataset de categorías."""
        print(f"\n{'#'*80}")
        print("📋 ANÁLISIS DEL DATASET: CATEGORÍAS DE PRODUCTOS")
        print(f"{'#'*80}")

        print("\n📂 Cargando datos de categorías...")
        df_categories = self.data_loader.load_categories()
        df_categories.cache()
        print("✅ Datos cargados correctamente\n")

        # Para tablas de dominio solo verificamos calidad de datos
        print("📝 NOTA: Este es un dataset de referencia (lookup table)")
        print("   Solo se verifica la estructura y calidad de datos\n")

        self.eda_analyzer.analyze_structure(df_categories, "categories")
        self.eda_analyzer.analyze_missing_values(df_categories, "categories")
        self.eda_analyzer.analyze_duplicates(df_categories, "categories")

        # Mostrar las categorías disponibles
        print("\n📋 Categorías disponibles en el sistema:")
        print("-" * 60)
        df_categories.orderBy("category_id").show(50, truncate=False)

        df_categories.unpersist()

    def run_product_categories_analysis(self):
        """Ejecuta análisis del dataset de productos-categorías."""
        print(f"\n{'#'*80}")
        print("📦 ANÁLISIS DEL DATASET: RELACIÓN PRODUCTOS-CATEGORÍAS")
        print(f"{'#'*80}")

        print("\n📂 Cargando datos de productos-categorías...")
        df_products = self.data_loader.load_product_categories()
        df_products.cache()
        print("✅ Datos cargados correctamente\n")

        self.eda_analyzer.analyze_structure(df_products, "product_categories")
        self.eda_analyzer.analyze_missing_values(df_products, "product_categories")
        self.eda_analyzer.analyze_duplicates(
            df_products, "product_categories", subset=["product_id"]
        )

        # Los IDs (product_id, category_id) no tienen significado estadístico
        # Solo analizamos cardinalidad y distribución
        print("\n🔢 Análisis de cardinalidad de IDs:")
        print("-" * 60)
        print(
            f"   📦 Productos únicos: {df_products.select('product_id').distinct().count():,}"
        )
        print(
            f"   📋 Categorías únicas: {df_products.select('category_id').distinct().count():,}"
        )

        # Análisis de distribución: productos por categoría
        print("\n📊 Distribución de productos por categoría:")
        df_with_categories = df_products.join(
            self.data_loader.load_categories(), "category_id", "left"
        )

        from pyspark.sql.functions import count, desc

        products_per_category = (
            df_with_categories.groupBy("category_name")
            .agg(count("*").alias("num_productos"))
            .orderBy(desc("num_productos"))
        )

        print("\nTop 15 categorías con más productos:")
        products_per_category.show(15, truncate=False)

        # Generar visualizaciones
        print("\n📊 Generando visualizaciones...")
        self.visualizer.plot_category_distribution(
            df_with_categories, "product_categories", top_n=15
        )

        # Gráfica de productos por categoría
        self.visualizer.plot_top_items(
            df_with_categories,
            "category_name",
            "products_per_category",
            top_n=20,
            title="Distribución de Productos por Categoría",
            ylabel="Categoría",
        )

        df_products.unpersist()

    def run_transactions_analysis(self, sample_size: int = None):
        """
        Ejecuta análisis del dataset de transacciones.

        Args:
            sample_size: Número de filas a muestrear (None = todas)
        """
        print(f"\n{'#'*80}")
        print("🛒 ANÁLISIS DEL DATASET: TRANSACCIONES")
        print(f"{'#'*80}")

        stores = self.data_loader.get_available_stores()
        print(f"\n🏪 Tiendas disponibles: {', '.join(stores)}")

        print("\n📂 Cargando datos de transacciones...")
        df_transactions = self.data_loader.load_transactions()

        if sample_size:
            print(f"⚠️ Tomando muestra de {sample_size:,} registros...")
            df_transactions = df_transactions.limit(sample_size)

        df_transactions.cache()
        print("✅ Datos cargados correctamente\n")

        self.eda_analyzer.analyze_structure(df_transactions, "transactions")
        self.eda_analyzer.analyze_missing_values(df_transactions, "transactions")
        self.eda_analyzer.analyze_duplicates(df_transactions, "transactions")

        # Analizar cantidad de productos por transacción
        from pyspark.sql.functions import size, split, trim, col
        print("\n📊 Análisis de productos por transacción:")
        print("-" * 60)
        df_with_count = df_transactions.withColumn(
            "num_productos", size(split(trim(col("products")), " "))
        )
        self.eda_analyzer.analyze_numeric_columns(
            df_with_count, "transactions", numeric_columns=["num_productos"]
        )

        # Analizamos cardinalidad y patrones de uso
        print("\n🔢 Análisis de cardinalidad de IDs:")
        print("-" * 60)
        print(
            f"   🏪 Tiendas únicas: {df_transactions.select('store_id').distinct().count():,}"
        )
        print(
            f"   👥 Clientes únicos: {df_transactions.select('customer_id').distinct().count():,}"
        )
        print(f"   🛒 Transacciones totales: {df_transactions.count():,}")

        # Análisis de frecuencia de clientes (top clientes por número de transacciones)
        print("\n🏆 Top 10 clientes con más transacciones:")
        from pyspark.sql.functions import count, desc

        df_transactions.groupBy("customer_id").agg(
            count("*").alias("num_transacciones")
        ).orderBy(desc("num_transacciones")).show(10, truncate=False)

        self.eda_analyzer.analyze_categorical_columns(
            df_transactions,
            "transactions",
            categorical_columns=["transaction_date"],
            top_n=15,
        )

        # Generar visualizaciones
        print("\n📊 Generando visualizaciones...")
        self.visualizer.plot_top_items(
            df_transactions,
            "customer_id",
            "transactions",
            top_n=20,
            title="Top 20 Clientes con Más Transacciones",
            ylabel="Customer ID",
        )
        self.visualizer.plot_temporal_trend(
            df_transactions, "transaction_date", "transactions", sample_dates=None
        )

        df_transactions.unpersist()

    def run_transactions_exploded_analysis(self, sample_size: int = None):
        """
        Ejecuta análisis de transacciones con productos explodidos.

        Args:
            sample_size: Número de transacciones a muestrear antes de explodir
        """
        print(f"\n{'#'*80}")
        print(
            "🎯 ANÁLISIS DEL DATASET: TRANSACCIONES EXPLODIDAS (DETALLE POR PRODUCTO)"
        )
        print(f"{'#'*80}")

        print("\n📂 Cargando y procesando transacciones...")
        df_transactions = self.data_loader.load_transactions()

        if sample_size:
            print(f"⚠️ Tomando muestra de {sample_size:,} transacciones...")
            df_transactions = df_transactions.limit(sample_size)

        print("🔄 Explodiendo productos (un producto por fila)...")
        df_exploded = self.data_loader.explode_transactions(df_transactions)
        df_exploded.cache()
        print("✅ Datos procesados correctamente\n")

        self.eda_analyzer.analyze_structure(df_exploded, "transactions_exploded")
        self.eda_analyzer.analyze_missing_values(df_exploded, "transactions_exploded")
        self.eda_analyzer.analyze_duplicates(df_exploded, "transactions_exploded")

        # Análisis de cardinalidad y frecuencias (no estadísticas descriptivas de IDs)
        print("\n🔢 Análisis de cardinalidad de IDs:")
        print("-" * 60)
        print(
            f"   📦 Productos únicos: {df_exploded.select('product_id').distinct().count():,}"
        )
        print(
            f"   👥 Clientes únicos: {df_exploded.select('customer_id').distinct().count():,}"
        )
        print(
            f"   🏪 Tiendas únicas: {df_exploded.select('store_id').distinct().count():,}"
        )
        print(f"   🛍️ Registros totales (productos vendidos): {df_exploded.count():,}")

        # Top productos más vendidos
        print("\n🏆 Top 15 productos más vendidos:")
        from pyspark.sql.functions import count, desc

        df_exploded.groupBy("product_id").agg(
            count("*").alias("veces_vendido")
        ).orderBy(desc("veces_vendido")).show(15, truncate=False)

        self.eda_analyzer.analyze_categorical_columns(
            df_exploded,
            "transactions_exploded",
            categorical_columns=["transaction_date"],
            top_n=15,
        )

        # Generar visualizaciones
        print("\n📊 Generando visualizaciones...")
        self.visualizer.plot_top_items(
            df_exploded,
            "product_id",
            "transactions_exploded",
            top_n=20,
            title="Top 20 Productos Más Vendidos",
            ylabel="Product ID",
        )
        self.visualizer.plot_temporal_trend(
            df_exploded, "transaction_date", "transactions_exploded", sample_dates=None
        )

        df_exploded.unpersist()

    def finalize(self):
        """Finaliza el pipeline."""
        print(f"\n{'='*80}")
        print("🏁 FINALIZANDO PIPELINE")
        print(f"{'='*80}\n")

        self.eda_analyzer.generate_summary_report()

        # Crear resumen de visualizaciones generadas
        print("\n📊 Resumen de visualizaciones:")
        self.visualizer.create_summary_report()

        end_time = datetime.now()
        duration = end_time - self.start_time

        print(f"\n⏰ Hora de finalización: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"⌛ Tiempo total de ejecución: {duration}")

        print("\n🛑 Deteniendo sesión de Spark...")
        stop_spark_session(self.spark)
        print("✅ Sesión de Spark detenida correctamente\n")

        print(f"{'='*80}")
        print("🎉 PIPELINE COMPLETADO EXITOSAMENTE")
        print(f"{'='*80}\n")
