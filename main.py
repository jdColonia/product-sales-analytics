"""
Script principal para ejecutar el pipeline de análisis exploratorio de datos.

Este script ejecuta el pipeline completo de EDA para los datasets
de ventas de productos, incluyendo análisis de categorías, productos
y transacciones.
"""

import sys
from src.pipeline import SalesAnalyticsPipeline, stop_spark_session


def main():
    """Función principal para ejecutar el pipeline."""

    pipeline = SalesAnalyticsPipeline(app_name="SalesAnalytics-EDA")

    try:
        # Inicializar pipeline
        pipeline.initialize()

        # Ejecutar análisis de categorías
        print("\n📋 PASO 1/7: Analizando categorías de productos...")
        pipeline.run_categories_analysis()

        # Ejecutar análisis de productos-categorías
        print("\n📦 PASO 2/7: Analizando relación productos-categorías...")
        pipeline.run_product_categories_analysis()

        # Ejecutar análisis de transacciones
        print("\n🛒 PASO 3/7: Analizando transacciones...")
        pipeline.run_transactions_analysis(sample_size=None)

        # Ejecutar análisis de transacciones explodidas
        print("\n🎯 PASO 4/7: Analizando transacciones detalladas por producto...")
        pipeline.run_transactions_exploded_analysis(sample_size=None)

        # Ejecutar análisis avanzados (temporal, clientes, productos)
        pipeline.run_advanced_analysis()

        # Finalizar
        pipeline.finalize()

        print("\n🎉 ANÁLISIS COMPLETADO CON ÉXITO")
        print("\n📊 Las gráficas se han guardado en: output/plots/\n")

        return 0

    except KeyboardInterrupt:
        print("\n\n⚠️ ADVERTENCIA: Análisis interrumpido por el usuario")
        if pipeline.spark:
            stop_spark_session(pipeline.spark)
        return 1

    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback

        traceback.print_exc()

        if pipeline.spark:
            stop_spark_session(pipeline.spark)
        return 1


if __name__ == "__main__":
    sys.exit(main())
