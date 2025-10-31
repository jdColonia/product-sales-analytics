"""
Módulo para análisis exploratorio de datos (EDA)
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    isnan,
    isnull,
    when,
    mean,
    stddev,
    min,
    max,
    percentile_approx,
    mode,
    sum as spark_sum,
    desc,
    asc,
)
from typing import Dict, List, Any, Optional
import builtins
import json


class EDAAnalyzer:
    """Clase para realizar análisis exploratorio de datos"""

    def __init__(self, spark: SparkSession):
        """
        Inicializa el analizador EDA

        Args:
            spark: Sesión de Spark
        """
        self.spark = spark
        self.results = {}

    def analyze_structure(self, df: DataFrame, dataset_name: str) -> Dict[str, Any]:
        """
        Analiza la estructura básica del dataset

        Args:
            df: DataFrame a analizar
            dataset_name: Nombre del dataset

        Returns:
            Diccionario con información estructural
        """
        print(f"\n{'='*80}")
        print(f"ANÁLISIS DE ESTRUCTURA: {dataset_name}")
        print(f"{'='*80}\n")

        # Número de registros y columnas
        num_rows = df.count()
        num_cols = len(df.columns)

        print(f"Dimensiones del dataset:")
        print(f"   Número de registros: {num_rows:,}")
        print(f"   Número de columnas: {num_cols}")

        # Tipos de datos
        print(f"\nTipos de datos:")
        schema_info = []
        for field in df.schema.fields:
            dtype = str(field.dataType)
            print(f"   {field.name}: {dtype}")
            schema_info.append(
                {"column": field.name, "type": dtype, "nullable": field.nullable}
            )

        # Vista previa de datos
        print(f"\nVista previa de los datos (primeras 5 filas):")
        df.show(5, truncate=False)

        structure_info = {
            "dataset_name": dataset_name,
            "num_rows": num_rows,
            "num_columns": num_cols,
            "columns": df.columns,
            "schema": schema_info,
        }

        self.results[f"{dataset_name}_structure"] = structure_info
        return structure_info

    def analyze_missing_values(
        self, df: DataFrame, dataset_name: str
    ) -> Dict[str, Any]:
        """
        Analiza valores faltantes y nulos

        Args:
            df: DataFrame a analizar
            dataset_name: Nombre del dataset

        Returns:
            Diccionario con información de valores faltantes
        """
        print(f"\n{'='*80}")
        print(f"ANÁLISIS DE VALORES FALTANTES: {dataset_name}")
        print(f"{'='*80}\n")

        total_rows = df.count()
        missing_info = []

        for column in df.columns:
            # Contar nulos
            null_count = df.filter(col(column).isNull()).count()
            null_percentage = (null_count / total_rows) * 100 if total_rows > 0 else 0

            missing_info.append(
                {
                    "column": column,
                    "null_count": null_count,
                    "null_percentage": round(null_percentage, 2),
                }
            )

            if null_count > 0:
                print(f"ADVERTENCIA - {column}:")
                print(f"   Valores nulos: {null_count:,} ({null_percentage:.2f}%)")

        if all(info["null_count"] == 0 for info in missing_info):
            print("No se encontraron valores faltantes en ninguna columna")

        result = {
            "dataset_name": dataset_name,
            "total_rows": total_rows,
            "missing_info": missing_info,
        }

        self.results[f"{dataset_name}_missing"] = result
        return result

    def analyze_duplicates(
        self, df: DataFrame, dataset_name: str, subset: List[str] = None
    ) -> Dict[str, Any]:
        """
        Analiza registros duplicados

        Args:
            df: DataFrame a analizar
            dataset_name: Nombre del dataset
            subset: Columnas a considerar para duplicados (None = todas)

        Returns:
            Diccionario con información de duplicados
        """
        print(f"\n{'='*80}")
        print(f"ANÁLISIS DE DUPLICADOS: {dataset_name}")
        print(f"{'='*80}\n")

        total_rows = df.count()

        # Contar filas únicas
        if subset:
            distinct_rows = df.dropDuplicates(subset).count()
        else:
            distinct_rows = df.distinct().count()

        duplicates = total_rows - distinct_rows
        duplicate_percentage = (duplicates / total_rows) * 100 if total_rows > 0 else 0

        print(f"Análisis de duplicados:")
        print(f"   Total de registros: {total_rows:,}")
        print(f"   Registros únicos: {distinct_rows:,}")
        print(f"   Duplicados: {duplicates:,} ({duplicate_percentage:.2f}%)")

        if duplicates > 0:
            print(f"ADVERTENCIA: Se encontraron {duplicates:,} registros duplicados")
            
            # Mostrar ejemplos de duplicados
            print(f"\n📋 Ejemplos de registros duplicados:")
            print("-" * 60)
            
            from pyspark.sql.functions import count as spark_count
            
            # Encontrar registros que aparecen más de una vez
            if subset:
                # Duplicados basados en columnas específicas
                duplicate_examples = df.groupBy(subset).agg(
                    spark_count("*").alias("veces_repetido")
                ).filter(col("veces_repetido") > 1).orderBy(desc("veces_repetido"))
            else:
                # Duplicados exactos (todas las columnas)
                duplicate_examples = df.groupBy(*df.columns).agg(
                    spark_count("*").alias("veces_repetido")
                ).filter(col("veces_repetido") > 1).orderBy(desc("veces_repetido"))
            
            print(f"\nMostrando primeros 10 casos (ordenados por frecuencia):")
            duplicate_examples.show(10, truncate=False)
            
        else:
            print("No se encontraron duplicados")

        result = {
            "dataset_name": dataset_name,
            "total_rows": total_rows,
            "distinct_rows": distinct_rows,
            "duplicates": duplicates,
            "duplicate_percentage": round(duplicate_percentage, 2),
            "subset_columns": subset,
        }

        self.results[f"{dataset_name}_duplicates"] = result
        return result

    def analyze_numeric_columns(
        self, df: DataFrame, dataset_name: str, numeric_columns: List[str] = None
    ) -> Dict[str, Any]:
        """
        Analiza estadísticas descriptivas para columnas numéricas

        Args:
            df: DataFrame a analizar
            dataset_name: Nombre del dataset
            numeric_columns: Lista de columnas numéricas (None = auto-detectar)

        Returns:
            Diccionario con estadísticas numéricas
        """
        print(f"\n{'='*80}")
        print(f"ANÁLISIS DE VARIABLES NUMÉRICAS: {dataset_name}")
        print(f"{'='*80}\n")

        # Auto-detectar columnas numéricas si no se especifican
        if numeric_columns is None:
            numeric_columns = [
                field.name
                for field in df.schema.fields
                if str(field.dataType)
                in ["IntegerType", "LongType", "DoubleType", "FloatType"]
            ]

        if not numeric_columns:
            print("ADVERTENCIA: No se encontraron columnas numéricas para analizar")
            return {}

        stats_results = {}

        for column in numeric_columns:
            print(f"\nColumna: {column}")
            print("-" * 60)

            # Calcular estadísticas
            stats = df.select(
                count(col(column)).alias("count"),
                mean(col(column)).alias("mean"),
                stddev(col(column)).alias("stddev"),
                min(col(column)).alias("min"),
                max(col(column)).alias("max"),
                percentile_approx(col(column), 0.25).alias("q25"),
                percentile_approx(col(column), 0.50).alias("median"),
                percentile_approx(col(column), 0.75).alias("q75"),
            ).collect()[0]

            # Calcular rango intercuartílico (IQR) y outliers
            q25 = stats["q25"] if stats["q25"] else 0
            q75 = stats["q75"] if stats["q75"] else 0
            iqr = q75 - q25
            lower_bound = q25 - 1.5 * iqr
            upper_bound = q75 + 1.5 * iqr

            outliers_count = df.filter(
                (col(column) < lower_bound) | (col(column) > upper_bound)
            ).count()

            total_count = stats["count"]
            outliers_percentage = (
                (outliers_count / total_count * 100) if total_count > 0 else 0
            )

            # Mostrar resultados
            print(f"   Count:                {stats['count']:,}")
            print(
                f"   Media:                {stats['mean']:.2f}"
                if stats["mean"]
                else "   Media:                N/A"
            )
            print(
                f"   Desviación estándar:  {stats['stddev']:.2f}"
                if stats["stddev"]
                else "   Desviación estándar:  N/A"
            )
            print(f"   Mínimo:               {stats['min']}")
            print(f"   Q1 (25%):             {stats['q25']}")
            print(f"   Mediana (50%):        {stats['median']}")
            print(f"   Q3 (75%):             {stats['q75']}")
            print(f"   Máximo:               {stats['max']}")
            print(f"   IQR:                  {iqr:.2f}")
            print(
                f"   Outliers:             {outliers_count:,} ({outliers_percentage:.2f}%)"
            )

            stats_results[column] = {
                "count": stats["count"],
                "mean": float(stats["mean"]) if stats["mean"] else None,
                "stddev": float(stats["stddev"]) if stats["stddev"] else None,
                "min": float(stats["min"]) if stats["min"] else None,
                "max": float(stats["max"]) if stats["max"] else None,
                "q25": float(stats["q25"]) if stats["q25"] else None,
                "median": float(stats["median"]) if stats["median"] else None,
                "q75": float(stats["q75"]) if stats["q75"] else None,
                "iqr": float(iqr),
                "outliers_count": outliers_count,
                "outliers_percentage": round(outliers_percentage, 2),
                "outlier_bounds": {
                    "lower": float(lower_bound),
                    "upper": float(upper_bound),
                },
            }

        result = {
            "dataset_name": dataset_name,
            "numeric_columns": numeric_columns,
            "statistics": stats_results,
        }

        self.results[f"{dataset_name}_numeric"] = result
        return result

    def analyze_outliers(
        self,
        df: DataFrame,
        dataset_name: str,
        column: str,
        metric_name: Optional[str] = None,
        context_columns: Optional[List[str]] = None,
        top_n: int = 10,
    ) -> Dict[str, Any]:
        """
        Detecta y muestra outliers utilizando el método del rango intercuartílico (IQR).

        Args:
            df: DataFrame de Spark con la métrica a analizar.
            dataset_name: Nombre del dataset base.
            column: Columna numérica sobre la que se detectarán outliers.
            metric_name: Nombre descriptivo de la métrica (opcional).
            context_columns: Columnas adicionales a mostrar en los ejemplos.
            top_n: Número de ejemplos de outliers a mostrar por cada extremo.

        Returns:
            Diccionario con la información del análisis de outliers.
        """

        metric_label = metric_name or column
        selected_columns = list(context_columns) if context_columns else []
        if column not in selected_columns:
            selected_columns.append(column)

        df_to_analyze = df.select(*selected_columns).filter(col(column).isNotNull())

        print(f"\n{'='*80}")
        print(f"ANÁLISIS DE OUTLIERS: {dataset_name} -> {metric_label}")
        print(f"{'='*80}\n")

        total_rows = df_to_analyze.count()

        if total_rows == 0:
            print("ADVERTENCIA: No hay datos válidos para analizar outliers")
            result = {
                "dataset_name": dataset_name,
                "metric_name": metric_label,
                "column": column,
                "total_rows": 0,
                "outliers_count": 0,
                "outliers_percentage": 0.0,
            }
            self.results[f"{dataset_name}_outliers_{column}"] = result
            return result

        stats = df_to_analyze.select(
            count(col(column)).alias("count"),
            mean(col(column)).alias("mean"),
            stddev(col(column)).alias("stddev"),
            min(col(column)).alias("min"),
            percentile_approx(col(column), 0.25).alias("q25"),
            percentile_approx(col(column), 0.50).alias("median"),
            percentile_approx(col(column), 0.75).alias("q75"),
            max(col(column)).alias("max"),
        ).collect()[0]

        q25 = stats["q25"]
        q75 = stats["q75"]

        if q25 is None or q75 is None:
            print("ADVERTENCIA: No fue posible calcular el IQR por falta de datos")
            result = {
                "dataset_name": dataset_name,
                "metric_name": metric_label,
                "column": column,
                "total_rows": total_rows,
                "outliers_count": 0,
                "outliers_percentage": 0.0,
            }
            self.results[f"{dataset_name}_outliers_{column}"] = result
            return result

        iqr = q75 - q25

        if iqr == 0:
            print("Nota: IQR = 0, no se detectaron outliers con este método")
            result = {
                "dataset_name": dataset_name,
                "metric_name": metric_label,
                "column": column,
                "total_rows": total_rows,
                "iqr": float(iqr),
                "lower_bound": float(q25),
                "upper_bound": float(q75),
                "outliers_count": 0,
                "outliers_percentage": 0.0,
            }
            self.results[f"{dataset_name}_outliers_{column}"] = result
            return result

        lower_bound = q25 - 1.5 * iqr
        upper_bound = q75 + 1.5 * iqr

        outliers_df = df_to_analyze.filter(
            (col(column) < lower_bound) | (col(column) > upper_bound)
        )

        outliers_count = outliers_df.count()
        outliers_percentage = (outliers_count / total_rows * 100) if total_rows > 0 else 0

        print(f"Métrica analizada: {metric_label}")
        print(f"   Total de registros evaluados: {total_rows:,}")
        print(
            f"   Media:                  {stats['mean']:.2f}"
            if stats["mean"] is not None
            else "   Media:                  N/A"
        )
        print(
            f"   Desviación estándar:    {stats['stddev']:.2f}"
            if stats["stddev"] is not None
            else "   Desviación estándar:    N/A"
        )
        print(f"   Mínimo:                 {stats['min']}")
        print(f"   Q1 (25%):               {q25}")
        print(f"   Mediana (50%):          {stats['median']}")
        print(f"   Q3 (75%):               {q75}")
        print(f"   Máximo:                 {stats['max']}")
        print(f"   IQR:                    {iqr:.2f}")
        print(f"   Límite inferior:        {lower_bound:.2f}")
        print(f"   Límite superior:        {upper_bound:.2f}")
        print(
            f"   Outliers detectados:    {outliers_count:,} ({outliers_percentage:.2f}%)"
        )

        lower_outliers_df = outliers_df.filter(col(column) < lower_bound)
        lower_outliers_count = lower_outliers_df.count()
        upper_outliers_count = outliers_count - lower_outliers_count

        if upper_outliers_count > 0:
            print(
                f"\n📈 Principales {builtins.min(top_n, upper_outliers_count)} outliers altos (>{upper_bound:.2f}):"
            )
            upper_outliers_df = outliers_df.filter(col(column) > upper_bound)
            upper_outliers_df.orderBy(col(column).desc()).show(
                top_n, truncate=False
            )

        if lower_outliers_count > 0:
            print(
                f"\n📉 Principales {builtins.min(top_n, lower_outliers_count)} outliers bajos (<{lower_bound:.2f}):"
            )
            lower_outliers_df.orderBy(col(column).asc()).show(top_n, truncate=False)

        if outliers_count == 0:
            print("\nNo se detectaron outliers con el criterio IQR para esta métrica.")

        result = {
            "dataset_name": dataset_name,
            "metric_name": metric_label,
            "column": column,
            "total_rows": int(total_rows),
            "mean": float(stats["mean"]) if stats["mean"] is not None else None,
            "stddev": float(stats["stddev"]) if stats["stddev"] is not None else None,
            "min": float(stats["min"]) if stats["min"] is not None else None,
            "q25": float(q25),
            "median": float(stats["median"]) if stats["median"] is not None else None,
            "q75": float(q75),
            "max": float(stats["max"]) if stats["max"] is not None else None,
            "iqr": float(iqr),
            "lower_bound": float(lower_bound),
            "upper_bound": float(upper_bound),
            "outliers_count": int(outliers_count),
            "outliers_percentage": round(outliers_percentage, 2),
            "upper_outliers_count": int(upper_outliers_count),
            "lower_outliers_count": int(lower_outliers_count),
        }

        self.results[f"{dataset_name}_outliers_{column}"] = result
        return result

    def analyze_categorical_columns(
        self,
        df: DataFrame,
        dataset_name: str,
        categorical_columns: List[str] = None,
        top_n: int = 10,
    ) -> Dict[str, Any]:
        """
        Analiza estadísticas descriptivas para columnas categóricas

        Args:
            df: DataFrame a analizar
            dataset_name: Nombre del dataset
            categorical_columns: Lista de columnas categóricas (None = auto-detectar)
            top_n: Número de categorías más frecuentes a mostrar

        Returns:
            Diccionario con estadísticas categóricas
        """
        print(f"\n{'='*80}")
        print(f"ANÁLISIS DE VARIABLES CATEGÓRICAS: {dataset_name}")
        print(f"{'='*80}\n")

        # Auto-detectar columnas categóricas si no se especifican
        if categorical_columns is None:
            categorical_columns = [
                field.name
                for field in df.schema.fields
                if str(field.dataType) == "StringType"
            ]

        if not categorical_columns:
            print("ADVERTENCIA: No se encontraron columnas categóricas para analizar")
            return {}

        total_rows = df.count()
        categorical_results = {}

        for column in categorical_columns:
            print(f"\nColumna: {column}")
            print("-" * 60)

            # Contar valores distintos
            distinct_count = df.select(countDistinct(col(column))).collect()[0][0]

            print(f"   Valores únicos: {distinct_count:,}")

            # Calcular frecuencias
            freq_df = (
                df.groupBy(column)
                .agg(count("*").alias("frequency"))
                .withColumn("percentage", (col("frequency") / total_rows * 100))
                .orderBy(desc("frequency"))
            )

            # Mostrar top N categorías
            print(f"\n   Top {top_n} categorías más frecuentes:")
            top_categories = freq_df.limit(top_n).collect()

            frequencies = []
            for i, row in enumerate(top_categories, 1):
                value = row[column] if row[column] is not None else "NULL"
                freq = row["frequency"]
                pct = row["percentage"]
                print(f"   {i:2d}. {value}: {freq:,} ({pct:.2f}%)")

                frequencies.append(
                    {
                        "value": str(value),
                        "frequency": freq,
                        "percentage": round(pct, 2),
                    }
                )

            categorical_results[column] = {
                "distinct_count": distinct_count,
                "top_categories": frequencies,
                "total_count": total_rows,
            }

        result = {
            "dataset_name": dataset_name,
            "categorical_columns": categorical_columns,
            "statistics": categorical_results,
        }

        self.results[f"{dataset_name}_categorical"] = result
        return result

    def generate_summary_report(self):
        """Genera un resumen de todos los análisis realizados."""
        print(f"\n{'='*80}")
        print("📊 RESUMEN GENERAL DEL ANÁLISIS")
        print(f"{'='*80}\n")

        print(f"Total de análisis realizados: {len(self.results)}")
        print(f"\nAnálisis completados:")
        for key in self.results.keys():
            print(f"   ✅ {key}")
