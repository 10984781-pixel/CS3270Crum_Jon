import asyncio
from collections import Counter
import inspect
import logging
import sys

from weather_package import WeatherDataAnalyzer, WeatherDataFetcher

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s:%(name)s:%(message)s",
)
logger = logging.getLogger("weather_app")


class _FallbackSeries:
    def __init__(self, values):
        self._values = list(values)

    def dropna(self):
        return _FallbackSeries([value for value in self._values if value is not None])

    @property
    def empty(self):
        return len(self._values) == 0

    def mode(self):
        if self.empty:
            return _FallbackSeries([])
        counts = Counter(self._values)
        top_count = max(counts.values())
        mode_values = [value for value, seen in counts.items() if seen == top_count]
        return _FallbackSeries(sorted(mode_values))

    @property
    def iloc(self):
        values = self._values

        class _Indexer:
            def __getitem__(self, index):
                return values[index]

        return _Indexer()

    def min(self):
        return min(self._values)

    def max(self):
        return max(self._values)

    def mean(self):
        return sum(self._values) / len(self._values)

    def median(self):
        ordered = sorted(self._values)
        count = len(ordered)
        mid = count // 2
        if count % 2 == 0:
            return (ordered[mid - 1] + ordered[mid]) / 2
        return ordered[mid]

    def tolist(self):
        return list(self._values)


class _FallbackDataFrame:
    def __init__(self, data, row_count):
        self._data = data
        self.columns = list(data.keys())
        self._row_count = row_count

    def select_dtypes(self, include):
        return self

    def __getitem__(self, column):
        return _FallbackSeries(self._data[column])

    def __len__(self):
        return self._row_count


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _build_fallback_dataframe(rows):
    if not rows:
        return _FallbackDataFrame({}, 0)

    header = rows[0]
    data_rows = rows[1:]
    numeric_columns = {column: [] for column in header}

    for row in data_rows:
        for index, column in enumerate(header):
            value = row[index] if index < len(row) else None
            numeric = _to_float(value)
            if numeric is not None:
                numeric_columns[column].append(numeric)

    filtered = {column: values for column, values in numeric_columns.items() if values}
    return _FallbackDataFrame(filtered, len(data_rows))


async def _load_data_concurrently(fetcher):
    """Load CSV rows and DataFrame concurrently using non-blocking orchestration."""
    if (
        inspect.iscoroutinefunction(getattr(fetcher, "load_csv_builtin_async", None))
        and inspect.iscoroutinefunction(getattr(fetcher, "load_csv_pandas_async", None))
    ):
        load_tasks = (
            fetcher.load_csv_builtin_async(),
            fetcher.load_csv_pandas_async(),
        )
    else:
        load_tasks = (
            asyncio.to_thread(fetcher.load_csv_builtin),
            asyncio.to_thread(fetcher.load_csv_pandas),
        )

    rows_result, dataframe_result = await asyncio.gather(
        *load_tasks,
        return_exceptions=True,
    )
    if isinstance(rows_result, Exception):
        raise rows_result

    if isinstance(dataframe_result, Exception):
        logger.warning(
            "Pandas loading unavailable; using built-in fallback frame: %s",
            dataframe_result,
        )
        dataframe_result = _build_fallback_dataframe(rows_result)

    return rows_result, dataframe_result


def main():
    file_name = "AustraliaWeatherData/Weather Training Data.csv"

    try:
        fetcher = WeatherDataFetcher(file_name)

        row_count = sum(1 for _ in fetcher.iter_rows())
        logger.info("CSV generator count: %s", row_count)

        rows, df = asyncio.run(_load_data_concurrently(fetcher))
        print("CSV method:", len(rows))
        print("Pandas method:", len(df))

        analyzer = WeatherDataAnalyzer(df)
        print("Descriptive stats (first 5 numeric columns):")
        parallel_stats = analyzer.descriptive_stats_parallel()
        if isinstance(parallel_stats, dict):
            stats_iterator = iter(parallel_stats.items())
        else:
            stats_iterator = analyzer.iter_stats()
        shown = 0
        for column, stat_block in stats_iterator:
            print(f"{column}: {stat_block}")
            shown += 1
            if shown >= 5:
                break
    except Exception:
        logger.exception("Processing failed.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
