"""
Módulo para generación de visualizaciones del análisis exploratorio.

Este módulo crea gráficas para variables categóricas, frecuencias
y distribuciones, facilitando la interpretación de resultados.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, desc
import matplotlib.pyplot as plt
import seaborn as sns
import os
from typing import Optional


class DataVisualizer:
    """Clase para generar visualizaciones del análisis EDA."""

    def __init__(self, output_path: str = "output/plots"):
        """
        Inicializa el visualizador.

        Args:
            output_path: Ruta donde guardar las gráficas
        """
        self.output_path = output_path
        os.makedirs(output_path, exist_ok=True)

        # Configurar estilo de gráficas
        sns.set_style("whitegrid")
        plt.rcParams["figure.figsize"] = (12, 6)
        plt.rcParams["font.size"] = 10

    def plot_categorical_frequency(
        self,
        df: DataFrame,
        column: str,
        dataset_name: str,
        top_n: int = 15,
        title: Optional[str] = None,
    ):
        """
        Genera gráfico de barras para frecuencias de variable categórica.

        Args:
            df: DataFrame de Spark
            column: Nombre de la columna categórica
            dataset_name: Nombre del dataset (para nombre de archivo)
            top_n: Número de categorías más frecuentes a mostrar
            title: Título personalizado del gráfico
        """
        # Calcular frecuencias
        freq_data = (
            df.groupBy(column)
            .agg(count("*").alias("frequency"))
            .orderBy(desc("frequency"))
            .limit(top_n)
            .toPandas()
        )

        if freq_data.empty:
            print(f"ADVERTENCIA: No hay datos para graficar {column}")
            return

        # Crear gráfico
        plt.figure(figsize=(12, 6))
        bars = plt.bar(range(len(freq_data)), freq_data["frequency"], color="steelblue")

        # Personalizar
        plt.xlabel(column.replace("_", " ").title())
        plt.ylabel("Frecuencia")
        plot_title = title or f"Top {top_n} - {column.replace('_', ' ').title()}"
        plt.title(plot_title)
        plt.xticks(
            range(len(freq_data)),
            freq_data[column].astype(str),
            rotation=45,
            ha="right",
        )

        # Agregar valores sobre las barras
        for i, bar in enumerate(bars):
            height = bar.get_height()
            plt.text(
                bar.get_x() + bar.get_width() / 2.0,
                height,
                f"{int(height):,}",
                ha="center",
                va="bottom",
                fontsize=9,
            )

        plt.tight_layout()

        # Guardar
        filename = f"{dataset_name}_{column}_frequency.png"
        filepath = os.path.join(self.output_path, filename)
        plt.savefig(filepath, dpi=300, bbox_inches="tight")
        plt.close()

        print(f"   Gráfica guardada: {filepath}")

    def plot_top_items(
        self,
        df: DataFrame,
        group_column: str,
        dataset_name: str,
        top_n: int = 15,
        title: Optional[str] = None,
        ylabel: str = "Frecuencia",
    ):
        """
        Genera gráfico de barras horizontales para top items.

        Args:
            df: DataFrame de Spark
            group_column: Columna para agrupar
            dataset_name: Nombre del dataset
            top_n: Número de items a mostrar
            title: Título del gráfico
            ylabel: Etiqueta del eje Y
        """
        # Calcular top items
        top_data = (
            df.groupBy(group_column)
            .agg(count("*").alias("count"))
            .orderBy(desc("count"))
            .limit(top_n)
            .toPandas()
        )

        if top_data.empty:
            print(f"ADVERTENCIA: No hay datos para graficar {group_column}")
            return

        # Crear gráfico horizontal
        plt.figure(figsize=(10, 8))
        bars = plt.barh(
            range(len(top_data)), top_data["count"], color="forestgreen", alpha=0.7
        )

        # Personalizar
        plt.xlabel("Cantidad")
        plt.ylabel(ylabel)
        plot_title = title or f"Top {top_n} - {group_column.replace('_', ' ').title()}"
        plt.title(plot_title)
        plt.yticks(range(len(top_data)), top_data[group_column].astype(str), fontsize=9)

        # Agregar valores a la derecha de las barras
        for i, bar in enumerate(bars):
            width = bar.get_width()
            plt.text(
                width,
                bar.get_y() + bar.get_height() / 2.0,
                f" {int(width):,}",
                ha="left",
                va="center",
                fontsize=9,
            )

        plt.tight_layout()

        # Guardar
        filename = f"{dataset_name}_top_{group_column}.png"
        filepath = os.path.join(self.output_path, filename)
        plt.savefig(filepath, dpi=300, bbox_inches="tight")
        plt.close()

        print(f"   Gráfica guardada: {filepath}")

    def plot_category_distribution(
        self, df: DataFrame, dataset_name: str, top_n: int = 10
    ):
        """
        Genera gráfico de pastel para distribución de categorías.

        Args:
            df: DataFrame de Spark
            dataset_name: Nombre del dataset
            top_n: Número de categorías principales (resto se agrupa en "Otros")
        """
        # Calcular distribución
        dist_data = (
            df.groupBy("category_name")
            .agg(count("*").alias("count"))
            .orderBy(desc("count"))
            .toPandas()
        )

        if dist_data.empty:
            print("ADVERTENCIA: No hay datos para graficar distribución de categorías")
            return

        # Agrupar categorías pequeñas en "Otros"
        if len(dist_data) > top_n:
            top_categories = dist_data.head(top_n)
            others_sum = dist_data.iloc[top_n:]["count"].sum()
            others_row = {"category_name": "Otros", "count": others_sum}
            import pandas as pd

            dist_data = pd.concat(
                [top_categories, pd.DataFrame([others_row])], ignore_index=True
            )
        else:
            dist_data = dist_data.head(top_n)

        # Crear gráfico de pastel
        plt.figure(figsize=(10, 8))
        colors = sns.color_palette("Set3", len(dist_data))

        wedges, texts, autotexts = plt.pie(
            dist_data["count"],
            labels=dist_data["category_name"],
            autopct="%1.1f%%",
            colors=colors,
            startangle=90,
        )

        # Mejorar legibilidad
        for text in texts:
            text.set_fontsize(9)
        for autotext in autotexts:
            autotext.set_color("white")
            autotext.set_fontsize(9)
            autotext.set_weight("bold")

        plt.title(f"Distribución de Categorías - Top {top_n}")
        plt.axis("equal")
        plt.tight_layout()

        # Guardar
        filename = f"{dataset_name}_category_distribution.png"
        filepath = os.path.join(self.output_path, filename)
        plt.savefig(filepath, dpi=300, bbox_inches="tight")
        plt.close()

        print(f"   Gráfica guardada: {filepath}")

    def plot_temporal_trend(
        self, df: DataFrame, date_column: str, dataset_name: str, sample_dates: int = 30
    ):
        """
        Genera gráfico de línea para tendencia temporal.

        Args:
            df: DataFrame de Spark
            date_column: Columna de fecha
            dataset_name: Nombre del dataset
            sample_dates: Número de fechas a mostrar (para evitar sobrecarga)
        """
        # Calcular transacciones por fecha
        temporal_data = (
            df.groupBy(date_column)
            .agg(count("*").alias("count"))
            .orderBy(date_column)
            .limit(sample_dates)
            .toPandas()
        )

        if temporal_data.empty:
            print("ADVERTENCIA: No hay datos temporales para graficar")
            return

        # Crear gráfico
        plt.figure(figsize=(14, 6))
        plt.plot(
            temporal_data[date_column],
            temporal_data["count"],
            marker="o",
            linewidth=2,
            markersize=4,
            color="darkblue",
        )

        plt.xlabel("Fecha")
        plt.ylabel("Número de Transacciones")
        plt.title(f"Tendencia Temporal - {dataset_name.replace('_', ' ').title()}")
        plt.xticks(rotation=45, ha="right")
        plt.grid(True, alpha=0.3)
        plt.tight_layout()

        # Guardar
        filename = f"{dataset_name}_temporal_trend.png"
        filepath = os.path.join(self.output_path, filename)
        plt.savefig(filepath, dpi=300, bbox_inches="tight")
        plt.close()

        print(f"   Gráfica guardada: {filepath}")

    def create_summary_report(self):
        """Muestra un resumen de gráficas generadas en consola."""
        if not os.path.exists(self.output_path):
            print("   No se generaron gráficas")
            return

        files = [f for f in os.listdir(self.output_path) if f.endswith(".png")]

        if not files:
            print("   No se generaron gráficas")
            return

        print(f"   Total de gráficas generadas: {len(files)}")
        print(f"   Ubicación: {os.path.abspath(self.output_path)}")
        print("\n   Archivos generados:")
        for file in sorted(files):
            print(f"      • {file}")
