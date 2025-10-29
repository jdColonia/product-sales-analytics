"""
Módulo para carga y preparación de datos.

Este módulo proporciona funcionalidades para cargar y transformar
datasets desde archivos CSV utilizando Apache Spark.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode, split, trim
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os
from typing import List


class DataLoader:
    """Clase para cargar y preparar datasets"""

    def __init__(self, spark: SparkSession, data_path: str = "data"):
        """
        Inicializa el cargador de datos

        Args:
            spark: Sesión de Spark
            data_path: Ruta base de los datos
        """
        self.spark = spark
        self.data_path = data_path

    def load_categories(self) -> DataFrame:
        """
        Carga el catálogo de categorías

        Returns:
            DataFrame con categorías
        """
        categories_path = os.path.join(self.data_path, "products", "Categories.csv")

        # Definir esquema explícito
        schema = StructType(
            [
                StructField("category_id", IntegerType(), True),
                StructField("category_name", StringType(), True),
            ]
        )

        df = self.spark.read.csv(categories_path, sep="|", header=False, schema=schema)

        return df

    def load_product_categories(self) -> DataFrame:
        """
        Carga la relación productos-categorías

        Returns:
            DataFrame con relación productos-categorías
        """
        product_cat_path = os.path.join(
            self.data_path, "products", "ProductCategory.csv"
        )

        df = self.spark.read.csv(
            product_cat_path, sep="|", header=True, inferSchema=True
        )

        # Renombrar columnas para mayor claridad
        df = df.withColumnRenamed("v.Code_pr", "product_id").withColumnRenamed(
            "v.code", "category_id"
        )

        return df

    def load_transactions(self, store_ids: List[str] = None) -> DataFrame:
        """
        Carga archivos de transacciones

        Args:
            store_ids: Lista de IDs de tiendas a cargar. Si es None, carga todas.

        Returns:
            DataFrame con transacciones
        """
        transactions_path = os.path.join(self.data_path, "transactions")

        # Definir esquema explícito
        schema = StructType(
            [
                StructField("transaction_date", StringType(), True),
                StructField("store_id", IntegerType(), True),
                StructField("customer_id", IntegerType(), True),
                StructField("products", StringType(), True),
            ]
        )

        # Determinar qué archivos cargar
        if store_ids is None:
            # Obtener todos los archivos de transacciones
            available_stores = self.get_available_stores()
            files_to_load = [
                os.path.join(transactions_path, f"{store_id}_Tran.csv")
                for store_id in available_stores
            ]
        else:
            # Cargar solo las tiendas especificadas
            files_to_load = [
                os.path.join(transactions_path, f"{store_id}_Tran.csv")
                for store_id in store_ids
            ]

        # Cargar cada archivo individualmente y unirlos
        dataframes = []
        for file_path in files_to_load:
            if os.path.exists(file_path):
                df_temp = self.spark.read.csv(
                    file_path, sep="|", header=False, schema=schema
                )
                dataframes.append(df_temp)

        # Unir todos los DataFrames
        if not dataframes:
            raise FileNotFoundError("No se encontraron archivos de transacciones")

        df = dataframes[0]
        for df_temp in dataframes[1:]:
            df = df.union(df_temp)

        return df

    def explode_transactions(self, df: DataFrame) -> DataFrame:
        """
        Explode las transacciones para tener un producto por fila

        Args:
            df: DataFrame de transacciones

        Returns:
            DataFrame con productos explodidos
        """
        # Separar la columna de productos en array
        df_exploded = df.withColumn("product_array", split(trim(col("products")), " "))

        # Explotar el array para tener un producto por fila
        df_exploded = df_exploded.withColumn(
            "product_id", explode(col("product_array"))
        ).drop("products", "product_array")

        # Convertir product_id a integer
        df_exploded = df_exploded.withColumn(
            "product_id", col("product_id").cast(IntegerType())
        )

        return df_exploded

    def get_available_stores(self) -> List[str]:
        """
        Obtiene la lista de tiendas disponibles en los datos

        Returns:
            Lista de IDs de tiendas
        """
        transactions_path = os.path.join(self.data_path, "transactions")
        files = os.listdir(transactions_path)

        store_ids = []
        for file in files:
            if file.endswith("_Tran.csv"):
                store_id = file.replace("_Tran.csv", "")
                store_ids.append(store_id)

        return sorted(store_ids)
