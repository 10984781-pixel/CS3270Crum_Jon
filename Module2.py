"""
Phase 6: Functional programming + visualization for weather data.
"""

from __future__ import annotations

import csv
import logging
from functools import reduce
from pathlib import Path

logger = logging.getLogger("weather_functional")


def iter_weather_records(file_path):
    """Yield each CSV row as a dict."""
    path = Path(file_path)
    try:
        with path.open(mode="r", newline="", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            for row in reader:
                yield row
    except FileNotFoundError:
        logger.error("CSV file not found: %s", path)
        raise
    except PermissionError:
        logger.error("Permission denied reading CSV: %s", path)
        raise
    except UnicodeDecodeError:
        logger.error("Unable to decode CSV file as UTF-8: %s", path)
        raise
    except csv.Error:
        logger.error("CSV parsing error in file: %s", path)
        raise


def filter_rainy_days(records):
    """Filter records for days that were rainy."""
    return filter(lambda record: record.get("RainToday") == "Yes", records)


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def rainfall_stats(records):
    """Return (rainfall_values, total, average) for rainy days."""
    rainfall_values = list(
        filter(
            lambda value: value is not None,
            map(lambda record: _to_float(record.get("Rainfall")), records),
        )
    )
    total = reduce(lambda acc, value: acc + value, rainfall_values, 0.0)
    average = total / len(rainfall_values) if rainfall_values else 0.0
    return rainfall_values, total, average


def average_max_temp_by_location(records, top_n=5):
    """Return top locations by average max temp as (location, avg_temp)."""

    def reducer(acc, record):
        temp = _to_float(record.get("MaxTemp"))
        if temp is None:
            return acc
        location = record.get("Location", "Unknown")
        total, count = acc.get(location, (0.0, 0))
        acc[location] = (total + temp, count + 1)
        return acc

    totals = reduce(reducer, records, {})
    averages = {
        location: total / count
        for location, (total, count) in totals.items()
        if count
    }
    return sorted(averages.items(), key=lambda item: item[1], reverse=True)[:top_n]


def _get_plt(plt=None):
    if plt is not None:
        return plt
    try:
        import matplotlib.pyplot as pyplot
    except ImportError as exc:
        raise ImportError("matplotlib is required for plotting") from exc
    return pyplot


def plot_rainfall_hist(rainfall_values, plt=None):
    plt = _get_plt(plt)
    plt.figure(figsize=(8, 4))
    plt.hist(rainfall_values, bins=30, color="skyblue", edgecolor="black")
    plt.title("Rainfall Distribution on Rainy Days")
    plt.xlabel("Rainfall (mm)")
    plt.ylabel("Count")
    plt.tight_layout()
    return plt


def plot_top_locations_bar(top_locations, plt=None):
    plt = _get_plt(plt)
    labels = list(map(lambda item: item[0], top_locations))
    values = list(map(lambda item: item[1], top_locations))
    plt.figure(figsize=(8, 4))
    plt.bar(labels, values, color="seagreen")
    plt.title("Top Locations by Avg Max Temp")
    plt.xlabel("Location")
    plt.ylabel("Average Max Temp (C)")
    plt.xticks(rotation=30, ha="right")
    plt.tight_layout()
    return plt


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s:%(name)s:%(message)s",
    )

    file_name = "AustraliaWeatherData/Weather Training Data.csv"

    try:
        records = list(iter_weather_records(file_name))
        rainy_records = list(filter_rainy_days(records))
        rainfall_values, total_rain, average_rain = rainfall_stats(rainy_records)
        top_locations = average_max_temp_by_location(records, top_n=5)

        print(f"Rainy days: {len(rainy_records)}")
        print(f"Total rainfall on rainy days: {total_rain:.2f} mm")
        print(f"Average rainfall on rainy days: {average_rain:.2f} mm")
        print("Top locations by average max temp:")
        for location, avg_temp in top_locations:
            print(f"  {location}: {avg_temp:.2f} C")

        plot_rainfall_hist(rainfall_values)
        plot_top_locations_bar(top_locations)
        _get_plt().show()
    except Exception:
        logger.exception("Functional analysis failed.")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
