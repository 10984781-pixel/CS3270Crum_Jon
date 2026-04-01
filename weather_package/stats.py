"""Descriptive statistics helpers."""

from __future__ import annotations

from collections import Counter
from concurrent.futures import ProcessPoolExecutor
import logging

logger = logging.getLogger(__name__)


def _column_stats_from_values(payload):
    """Compute descriptive statistics for one numeric column."""
    column, values = payload
    if not values:
        return column, None

    numeric_values = [float(value) for value in values]
    ordered = sorted(numeric_values)
    count = len(numeric_values)
    total = sum(numeric_values)
    col_min = ordered[0]
    col_max = ordered[-1]
    mode_counts = Counter(numeric_values)
    top_count = max(mode_counts.values())
    mode_candidates = [value for value, seen in mode_counts.items() if seen == top_count]
    mode_value = min(mode_candidates)

    mid = count // 2
    if count % 2 == 0:
        median = (ordered[mid - 1] + ordered[mid]) / 2.0
    else:
        median = ordered[mid]

    return column, {
        "mean": float(total / count),
        "median": float(median),
        "mode": float(mode_value),
        "min": float(col_min),
        "max": float(col_max),
        "range": float(col_max - col_min),
    }


class StatsIterator:
    """Iterator over descriptive statistics for numeric columns."""

    def __init__(self, dataframe):
        self._numeric_df = dataframe.select_dtypes(include="number")
        self._columns = iter(self._numeric_df.columns)

    def __iter__(self):
        return self

    def __next__(self):
        column = next(self._columns)
        series = self._numeric_df[column].dropna()
        if series.empty:
            return self.__next__()

        mode_series = series.mode()
        mode_value = mode_series.iloc[0] if not mode_series.empty else None
        col_min = series.min()
        col_max = series.max()

        return column, {
            "mean": float(series.mean()),
            "median": float(series.median()),
            "mode": float(mode_value) if mode_value is not None else None,
            "min": float(col_min),
            "max": float(col_max),
            "range": float(col_max - col_min),
        }


class WeatherDataAnalyzer:
    """Compute descriptive statistics for weather data."""

    def __init__(self, dataframe):
        self.dataframe = dataframe

    def iter_stats(self):
        """Yield (column, stats_dict) for numeric columns."""
        if self.dataframe is None:
            logger.error("No DataFrame provided for analysis.")
            raise ValueError("DataFrame is required for analysis.")
        return StatsIterator(self.dataframe)

    def descriptive_stats(self):
        """Return mean, median, mode, min, max, and range for numeric columns."""
        stats = {}
        for column, stat_block in self.iter_stats():
            stats[column] = stat_block
        return stats

    def descriptive_stats_parallel(self, max_workers=None):
        """Return descriptive stats using multiprocessing across numeric columns."""
        if self.dataframe is None:
            logger.error("No DataFrame provided for analysis.")
            raise ValueError("DataFrame is required for analysis.")

        numeric_df = self.dataframe.select_dtypes(include="number")
        column_payloads = []
        for column in numeric_df.columns:
            series = numeric_df[column].dropna()
            if series.empty:
                continue
            column_payloads.append((column, list(series.tolist())))

        if not column_payloads:
            return {}

        if len(column_payloads) == 1:
            column, stats = _column_stats_from_values(column_payloads[0])
            return {column: stats}

        try:
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                result_pairs = executor.map(_column_stats_from_values, column_payloads)
                return {column: stats for column, stats in result_pairs if stats is not None}
        except (PermissionError, OSError) as exc:
            logger.warning(
                "Multiprocessing unavailable, using sequential stats fallback: %s",
                exc,
            )
            result_pairs = map(_column_stats_from_values, column_payloads)
            return {column: stats for column, stats in result_pairs if stats is not None}
