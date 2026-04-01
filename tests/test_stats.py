import unittest

from weather_package.stats import StatsIterator, WeatherDataAnalyzer


class FakeSeries:
    def __init__(self, values):
        self._values = list(values)

    def dropna(self):
        return FakeSeries([value for value in self._values if value is not None])

    @property
    def empty(self):
        return len(self._values) == 0

    def mode(self):
        if self.empty:
            return FakeSeries([])
        counts = {}
        for value in self._values:
            counts[value] = counts.get(value, 0) + 1
        max_count = max(counts.values())
        modes = [value for value, count in counts.items() if count == max_count]
        return FakeSeries(modes)

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

    def tolist(self):
        return list(self._values)

    def median(self):
        ordered = sorted(self._values)
        count = len(ordered)
        mid = count // 2
        if count % 2 == 0:
            return (ordered[mid - 1] + ordered[mid]) / 2
        return ordered[mid]


class FakeDataFrame:
    def __init__(self, data):
        self._data = data
        self.columns = list(data.keys())

    def select_dtypes(self, include):
        return self

    def __getitem__(self, column):
        return FakeSeries(self._data[column])


class TestWeatherDataAnalyzer(unittest.TestCase):
    def test_iter_stats_requires_dataframe(self):
        analyzer = WeatherDataAnalyzer(None)
        with self.assertRaises(ValueError):
            analyzer.iter_stats()

    def test_stats_iterator_skips_empty_columns(self):
        data = {"empty": [None, None], "values": [1, 2, 2, 3]}
        iterator = StatsIterator(FakeDataFrame(data))
        column, stats = next(iterator)
        self.assertEqual(column, "values")
        self.assertAlmostEqual(stats["mean"], 2.0)

    def test_stats_iterator_stop_iteration(self):
        data = {"empty": [None, None]}
        iterator = StatsIterator(FakeDataFrame(data))
        with self.assertRaises(StopIteration):
            next(iterator)

    def test_descriptive_stats_builds_dict(self):
        data = {"temps": [1, 2, 3, 4]}
        analyzer = WeatherDataAnalyzer(FakeDataFrame(data))
        stats = analyzer.descriptive_stats()
        self.assertIn("temps", stats)
        self.assertEqual(stats["temps"]["min"], 1.0)
        self.assertEqual(stats["temps"]["max"], 4.0)

    def test_descriptive_stats_parallel_requires_dataframe(self):
        analyzer = WeatherDataAnalyzer(None)
        with self.assertRaises(ValueError):
            analyzer.descriptive_stats_parallel()

    def test_descriptive_stats_parallel_matches_sequential(self):
        data = {
            "temps": [1, 2, 3, 4, 4],
            "rain": [0, 0, 1, 2, 3],
        }
        analyzer = WeatherDataAnalyzer(FakeDataFrame(data))
        sequential = analyzer.descriptive_stats()
        parallel = analyzer.descriptive_stats_parallel(max_workers=2)
        self.assertEqual(set(sequential.keys()), set(parallel.keys()))
        for column in sequential:
            self.assertEqual(sequential[column], parallel[column])
