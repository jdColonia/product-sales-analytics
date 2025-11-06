"""
Módulo para análisis de productos.
Incluye análisis de productos más vendidos y reglas de asociación (Market Basket Analysis).
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    avg,
    min as spark_min,
    max as spark_max,
    desc,
    collect_list,
    array_distinct,
    size,
    explode,
    split,
    trim,
    row_number,
    concat_ws,
)
from pyspark.ml.fpm import FPGrowth
from pyspark.sql.window import Window
from typing import Dict, Any, List
import os


class ProductAnalyzer:
    """Clase para realizar análisis de productos y reglas de asociación."""

    def __init__(self, spark: SparkSession):
        """
        Inicializa el analizador de productos.

        Args:
            spark: Sesión de Spark
        """
        self.spark = spark

    def analyze_top_products(
        self, df_exploded: DataFrame, top_n: int = 20
    ) -> DataFrame:
        """
        Analiza los productos más vendidos.

        Args:
            df_exploded: DataFrame con productos "explotados" (un producto por fila)
            top_n: Número de productos top a mostrar

        Returns:
            DataFrame con productos más vendidos
        """
        print("\n🏆 Análisis de Productos Más Vendidos")
        print("-" * 60)

        df_product_sales = (
            df_exploded.groupBy("product_id")
            .agg(count("*").alias("num_ventas"))
            .orderBy(desc("num_ventas"))
        )

        print(f"\n📊 Top {top_n} productos más vendidos:")
        df_product_sales.show(top_n, truncate=False)

        # Estadísticas generales
        stats = df_product_sales.select(
            avg("num_ventas").alias("promedio_ventas"),
            spark_min("num_ventas").alias("min_ventas"),
            spark_max("num_ventas").alias("max_ventas"),
        ).collect()[0]

        print(f"\n📊 Estadísticas de ventas por producto:")
        print(f"   Promedio de ventas por producto: {stats['promedio_ventas']:.2f}")
        print(f"   Producto menos vendido: {stats['min_ventas']} ventas")
        print(f"   Producto más vendido: {stats['max_ventas']} ventas")

        return df_product_sales

    def analyze_products_per_store(
        self, df_exploded: DataFrame, top_n: int = 10
    ) -> DataFrame:
        """
        Analiza productos más vendidos por tienda.

        Args:
            df_exploded: DataFrame con productos "explotados"
            top_n: Número de productos top por tienda

        Returns:
            DataFrame con productos más vendidos por tienda
        """
        print("\n🏪 Análisis de Productos por Tienda")
        print("-" * 60)

        df_store_products = (
            df_exploded.groupBy("store_id", "product_id")
            .agg(count("*").alias("num_ventas"))
            .orderBy("store_id", desc("num_ventas"))
        )

        # Obtener top productos por tienda
        window_spec = Window.partitionBy("store_id").orderBy(desc("num_ventas"))

        df_top_per_store = (
            df_store_products.withColumn("rank", row_number().over(window_spec))
            .filter(col("rank") <= top_n)
            .orderBy("store_id", "rank")
        )

        print(f"\n📊 Top {top_n} productos por tienda:")
        df_top_per_store.show(50, truncate=False)

        return df_top_per_store

    def prepare_baskets(self, df: DataFrame) -> DataFrame:
        """
        Prepara los datos en formato de "canastas" para Market Basket Analysis.

        Cada fila representa una transacción (canasta) con la lista de productos.

        Args:
            df: DataFrame con transacciones

        Returns:
            DataFrame con formato de canastas
        """
        print("\n🛒 Preparando datos para Market Basket Analysis")
        print("-" * 60)

        # Convertir string de productos a array
        df_baskets = df.withColumn("items", split(trim(col("products")), " ")).select(
            col("customer_id"), col("transaction_date"), col("store_id"), col("items")
        )

        # Estadísticas de las canastas
        df_basket_stats = df_baskets.withColumn("basket_size", size(col("items")))

        stats = df_basket_stats.select(
            count("*").alias("num_transacciones"),
            avg("basket_size").alias("avg_basket_size"),
            spark_min("basket_size").alias("min_basket_size"),
            spark_max("basket_size").alias("max_basket_size"),
        ).collect()[0]

        print(f"📊 Estadísticas de las canastas:")
        print(f"   Total de transacciones: {stats['num_transacciones']:,}")
        print(
            f"   Tamaño promedio de canasta: {stats['avg_basket_size']:.2f} productos"
        )
        print(f"   Canasta más pequeña: {stats['min_basket_size']} producto(s)")
        print(f"   Canasta más grande: {stats['max_basket_size']} productos")

        return df_baskets

    def market_basket_analysis(
        self,
        df: DataFrame,
        min_support: float = 0.01,
        min_confidence: float = 0.3,
        top_rules: int = 20,
    ) -> Dict[str, DataFrame]:
        """
        Realiza Market Basket Analysis usando FP-Growth.

        Encuentra patrones frecuentes y reglas de asociación entre productos.

        Args:
            df: DataFrame con transacciones
            min_support: Soporte mínimo (default: 0.01 = 1%)
            min_confidence: Confianza mínima para reglas (default: 0.3 = 30%)
            top_rules: Número de mejores reglas a mostrar

        Returns:
            Diccionario con itemsets frecuentes y reglas de asociación
        """
        print("\n🔍 Market Basket Analysis (FP-Growth)")
        print("-" * 60)
        print(f"⚙️ Parámetros:")
        print(f"   Soporte mínimo: {min_support*100:.1f}%")
        print(f"   Confianza mínima: {min_confidence*100:.1f}%")

        # Preparar canastas
        df_baskets = self.prepare_baskets(df)

        # Definir rutas de archivos
        output_dir = "output/data"
        freq_itemsets_path = os.path.join(output_dir, "fp_growth_freq_itemsets")
        association_rules_path = os.path.join(output_dir, "fp_growth_association_rules")
        
        # Crear directorio si no existe
        os.makedirs(output_dir, exist_ok=True)
        
        # Verificar si los archivos ya existen (puede ser directorio de Spark o archivo CSV individual)
        def check_csv_files_exist(path):
            """Verifica si existen archivos CSV en el directorio o como archivo individual."""
            # Verificar si es un archivo CSV individual
            if os.path.exists(path) and os.path.isfile(path) and path.endswith('.csv'):
                return True
            # Verificar si es un directorio con archivos CSV
            if os.path.exists(path) and os.path.isdir(path):
                files = os.listdir(path)
                # Spark puede crear archivos .csv o archivos sin extensión que empiezan con 'part-'
                csv_files = [f for f in files if f.endswith('.csv') or f.startswith('part-')]
                return len(csv_files) > 0
            return False
        
        # También verificar archivos CSV individuales (método alternativo con pandas)
        freq_itemsets_csv_path = os.path.join(output_dir, "fp_growth_freq_itemsets.csv")
        rules_csv_path = os.path.join(output_dir, "fp_growth_association_rules.csv")
        
        freq_itemsets_exists = check_csv_files_exist(freq_itemsets_path) or os.path.exists(freq_itemsets_csv_path)
        association_rules_exists = check_csv_files_exist(association_rules_path) or os.path.exists(rules_csv_path)
        
        if freq_itemsets_exists and association_rules_exists:
            print(f"\n📂 Cargando resultados de FP-Growth desde archivos existentes...")
            print(f"   Ruta: {output_dir}/")
            
            # Determinar qué archivo usar (directorio de Spark o CSV individual)
            freq_path_to_load = freq_itemsets_csv_path if os.path.exists(freq_itemsets_csv_path) else freq_itemsets_path
            rules_path_to_load = rules_csv_path if os.path.exists(rules_csv_path) else association_rules_path
            
            # Cargar itemsets frecuentes
            df_freq_itemsets = self.spark.read.csv(
                freq_path_to_load, 
                header=True, 
                inferSchema=True
            )
            # Convertir la columna items de string a array
            df_freq_itemsets = df_freq_itemsets.withColumn(
                "items", 
                split(trim(col("items")), ",")
            )
            
            # Cargar reglas de asociación
            df_rules = self.spark.read.csv(
                rules_path_to_load,
                header=True,
                inferSchema=True
            )
            # Convertir las columnas antecedent y consequent de string a array
            df_rules = df_rules.withColumn(
                "antecedent",
                split(trim(col("antecedent")), ",")
            ).withColumn(
                "consequent",
                split(trim(col("consequent")), ",")
            )
            
            total_rules = df_rules.count()
            print(f"✅ Resultados cargados exitosamente")
            print(f"📊 Itemsets Frecuentes encontrados: {df_freq_itemsets.count():,}")
            
            print(f"\n📋 Top itemsets más frecuentes:")
            df_freq_itemsets.orderBy(desc("freq")).show(20, truncate=False)
            
            print(f"📊 Reglas de Asociación encontradas: {total_rules:,}")
            
            # Crear un modelo dummy para mantener compatibilidad
            model = None
        else:
            # Aplicar FP-Growth
            print(f"\n🔄 Ejecutando algoritmo FP-Growth...")
            fpGrowth = FPGrowth(
                itemsCol="items", minSupport=min_support, minConfidence=min_confidence
            )

            model = fpGrowth.fit(df_baskets)

            # Obtener itemsets frecuentes
            df_freq_itemsets = model.freqItemsets
            print(f"\n📊 Itemsets Frecuentes encontrados: {df_freq_itemsets.count():,}")

            print(f"\n📋 Top itemsets más frecuentes:")
            df_freq_itemsets.orderBy(desc("freq")).show(20, truncate=False)

            # Obtener reglas de asociación
            df_rules = model.associationRules
            total_rules = df_rules.count()
            print(f"\n📊 Reglas de Asociación encontradas: {total_rules:,}")
            
            # Guardar resultados en CSV
            print(f"\n💾 Guardando resultados de FP-Growth en {output_dir}/...")
            
            # Convertir arrays a strings para guardar en CSV
            df_freq_itemsets_to_save = df_freq_itemsets.withColumn(
                "items",
                concat_ws(",", col("items"))
            )
            
            df_rules_to_save = df_rules.withColumn(
                "antecedent",
                concat_ws(",", col("antecedent"))
            ).withColumn(
                "consequent",
                concat_ws(",", col("consequent"))
            )
            
            # Intentar guardar usando Spark CSV, si falla usar pandas (compatible con Windows)
            try:
                # Guardar itemsets frecuentes
                df_freq_itemsets_to_save.coalesce(1).write.mode("overwrite").option("header", "true").csv(freq_itemsets_path)
                
                # Guardar reglas de asociación
                df_rules_to_save.coalesce(1).write.mode("overwrite").option("header", "true").csv(association_rules_path)
                
                print(f"✅ Resultados guardados exitosamente en {output_dir}/")
            except Exception as e:
                # Si falla con Spark (problema común en Windows), usar pandas como respaldo
                print(f"⚠️ Error al guardar con Spark CSV, usando método alternativo (pandas)...")
                import pandas as pd
                
                # Convertir a pandas y guardar directamente
                freq_itemsets_csv_path = os.path.join(output_dir, "fp_growth_freq_itemsets.csv")
                rules_csv_path = os.path.join(output_dir, "fp_growth_association_rules.csv")
                
                # Guardar itemsets frecuentes
                pdf_freq = df_freq_itemsets_to_save.toPandas()
                pdf_freq.to_csv(freq_itemsets_csv_path, index=False, encoding='utf-8')
                
                # Guardar reglas de asociación
                pdf_rules = df_rules_to_save.toPandas()
                pdf_rules.to_csv(rules_csv_path, index=False, encoding='utf-8')
                
                print(f"✅ Resultados guardados exitosamente usando método alternativo en {output_dir}/")

        if total_rules > 0:
            # Ordenar por lift (mejor métrica que confidence sola)
            df_rules_sorted = df_rules.orderBy(desc("lift"), desc("confidence"))

            print(f"\n🏆 Top {top_rules} Reglas de Asociación (ordenadas por Lift):")
            df_rules_sorted.show(top_rules, truncate=False)

            # Explicar las métricas
            print(f"\n📖 Interpretación de métricas:")
            print(f"   • Antecedent (SI): Producto(s) en la canasta")
            print(
                f"   • Consequent (ENTONCES): Producto(s) frecuentemente comprados juntos"
            )
            print(
                f"   • Confidence: Probabilidad de comprar 'consequent' dado 'antecedent'"
            )
            print(
                f"   • Lift > 1: Indica asociación positiva (compran juntos más de lo esperado)"
            )
            print(f"   • Lift = 1: Independientes")
            print(f"   • Lift < 1: Asociación negativa")

            # Filtrar mejores reglas (lift > 1.5 y confidence > 0.5)
            df_best_rules = df_rules_sorted.filter(
                (col("lift") > 1.5) & (col("confidence") > 0.5)
            )

            best_count = df_best_rules.count()
            if best_count > 0:
                print(
                    f"\n⭐ Reglas FUERTES (Lift > 1.5 y Confidence > 50%): {best_count}"
                )
                df_best_rules.show(20, truncate=False)
            else:
                print(
                    f"\n⚠️ No se encontraron reglas fuertes con los criterios especificados"
                )
                print(f"   Considera ajustar min_support o min_confidence")

        else:
            print(f"\n⚠️ No se encontraron reglas de asociación")
            print(f"   Considera reducir min_support o min_confidence")

        return {
            "frequent_itemsets": df_freq_itemsets,
            "association_rules": df_rules,
            "model": model,
        }

    def analyze_product_combinations(self, df: DataFrame, top_n: int = 20) -> DataFrame:
        """
        Analiza las combinaciones de productos más frecuentes en transacciones.

        Args:
            df: DataFrame con transacciones
            top_n: Número de combinaciones top a mostrar

        Returns:
            DataFrame con combinaciones más frecuentes
        """
        print("\n🔗 Análisis de Combinaciones de Productos")
        print("-" * 60)

        # Preparar datos
        df_combinations = (
            df.withColumn("items", split(trim(col("products")), " "))
            .withColumn("num_items", size(col("items")))
            .filter(col("num_items") >= 2)  # Solo combinaciones de 2+ productos
        )

        print(f"\n📊 Transacciones con múltiples productos:")
        print(f"   Total: {df_combinations.count():,}")

        # Agrupar por combinación exacta
        df_combo_freq = (
            df_combinations.groupBy("items")
            .agg(count("*").alias("frequency"))
            .orderBy(desc("frequency"))
        )

        print(f"\n🏆 Top {top_n} combinaciones más frecuentes:")
        df_combo_freq.show(top_n, truncate=False)

        return df_combo_freq

    def generate_product_summary(
        self,
        df: DataFrame,
        df_exploded: DataFrame,
        run_market_basket: bool = True,
        min_support: float = 0.01,
        min_confidence: float = 0.3,
    ) -> Dict[str, Any]:
        """
        Genera un resumen completo del análisis de productos.

        Args:
            df: DataFrame con transacciones (sin explotar)
            df_exploded: DataFrame con productos explotados
            run_market_basket: Si ejecutar Market Basket Analysis
            min_support: Soporte mínimo para FP-Growth
            min_confidence: Confianza mínima para reglas

        Returns:
            Diccionario con resumen
        """
        print("\n" + "=" * 60)
        print("🛍️ RESUMEN DE ANÁLISIS DE PRODUCTOS")
        print("=" * 60)

        # Análisis de productos más vendidos
        df_top_products = self.analyze_top_products(df_exploded, top_n=20)

        # Análisis por tienda
        df_store_products = self.analyze_products_per_store(df_exploded, top_n=10)

        # Análisis de combinaciones
        df_combinations = self.analyze_product_combinations(df, top_n=20)

        # Market Basket Analysis (opcional, puede ser costoso)
        market_basket_results = None
        if run_market_basket:
            try:
                market_basket_results = self.market_basket_analysis(
                    df,
                    min_support=min_support,
                    min_confidence=min_confidence,
                    top_rules=20,
                )
            except Exception as e:
                print(f"\n⚠️ Error en Market Basket Analysis: {str(e)}")
                print(f"   Continuando sin este análisis...")

        print("=" * 60)

        return {
            "top_products": df_top_products,
            "store_products": df_store_products,
            "combinations": df_combinations,
            "market_basket": market_basket_results,
        }
