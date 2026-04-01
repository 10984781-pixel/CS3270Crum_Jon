import unittest
from unittest.mock import patch

import Module1


class TestModule1(unittest.TestCase):
    @patch("Module1.WeatherDataAnalyzer")
    @patch("Module1.WeatherDataFetcher")
    def test_main_success(self, fetcher_cls, analyzer_cls):
        fetcher_instance = fetcher_cls.return_value
        fetcher_instance.iter_rows.return_value = iter([["a"], ["b"]])
        fetcher_instance.load_csv_builtin.return_value = [["a"], ["b"]]
        fake_df = [1, 2, 3]
        fetcher_instance.load_csv_pandas.return_value = fake_df

        analyzer_instance = analyzer_cls.return_value
        analyzer_instance.iter_stats.return_value = iter([("col", {"mean": 1.0})])

        result = Module1.main()
        self.assertEqual(result, 0)
        analyzer_cls.assert_called_once_with(fake_df)

    @patch("Module1.WeatherDataFetcher")
    def test_main_failure(self, fetcher_cls):
        fetcher_instance = fetcher_cls.return_value
        fetcher_instance.iter_rows.side_effect = RuntimeError("boom")

        result = Module1.main()
        self.assertEqual(result, 1)
