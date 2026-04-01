"""Minimal PySpark migration of the weather analysis app.

This script is designed for Google Colab or a local PySpark install.
It uses a local virtual Spark cluster via master("local[*]").
"""

from __future__ import annotations

from pathlib import Path

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
except ImportError as exc:  # pragma: no cover - depends on external runtime
    raise ImportError(
        "PySpark is required. In Colab run: pip install pyspark"
    ) from exc


def run_analysis(csv_path: str = "AustraliaWeatherData/Weather Training Data.csv") -> int:
    """Run a small weather analysis using PySpark DataFrames."""
    data_path = Path(csv_path)
    if not data_path.exists():
        print(f"Error: file not found -> {csv_path}")
        return 1

    spark = (
        SparkSession.builder.appName("CS3270-Weather-PySpark")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        weather_df = (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(str(data_path))
        )

        print("Spark session created successfully.")
        print(f"Rows loaded: {weather_df.count()}")
        print(f"Columns loaded: {len(weather_df.columns)}")
        print(f"Spark master: {spark.sparkContext.master}")
        print(f"Default parallelism: {spark.sparkContext.defaultParallelism}")

        rainy_df = weather_df.filter(F.col("RainToday") == "Yes")
        rainy_count = rainy_df.count()

        rainfall_summary = rainy_df.select(
            F.sum("Rainfall").alias("total_rainfall_mm"),
            F.avg("Rainfall").alias("avg_rainfall_mm"),
        ).first()

        top_locations = (
            weather_df.groupBy("Location")
            .agg(F.avg("MaxTemp").alias("avg_max_temp_c"))
            .orderBy(F.desc("avg_max_temp_c"))
            .limit(5)
        )

        print("\nRainy-day stats:")
        print(f"Rainy day count: {rainy_count}")
        print(f"Total rainfall (mm): {float(rainfall_summary['total_rainfall_mm']):.2f}")
        print(f"Average rainfall (mm): {float(rainfall_summary['avg_rainfall_mm']):.2f}")

        print("\nTop 5 locations by average max temp:")
        top_locations.show(truncate=False)

        print("\nQuick numeric summary:")
        weather_df.select("MinTemp", "MaxTemp", "Rainfall").summary(
            "count", "mean", "min", "max"
        ).show(truncate=False)

        output_path = "output/pyspark_top_locations_csv"
        top_locations.coalesce(1).write.mode("overwrite").option("header", True).csv(
            output_path
        )
        print(f"\nSaved result file to: {output_path}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(run_analysis())
