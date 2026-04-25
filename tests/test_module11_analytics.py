from io import StringIO

from phase9_webapp.analytics import build_analysis
from phase9_webapp.csv_data import filter_by_city, load_weather_data


def _sample_city_df(city="Sydney"):
    csv_text = """Location,Date,MinTemp,MaxTemp,Rainfall\nSydney,2024-01-01,18,31,2\nSydney,2024-02-01,20,33,10\nSydney,2024-03-01,17,29,25\nMelbourne,2024-01-01,14,26,1\n"""
    df = load_weather_data(StringIO(csv_text))
    return filter_by_city(df, city)


def test_temperature_analysis_returns_line_chart():
    city_df = _sample_city_df("Sydney")

    result = build_analysis(city_df, "temperature", "Sydney")

    assert result["chart"]["data"][0]["type"] == "scatter"
    assert "Sydney" in result["summary"]


def test_rainfall_analysis_returns_bar_chart():
    city_df = _sample_city_df("Sydney")

    result = build_analysis(city_df, "rainfall", "Sydney")

    assert result["chart"]["data"][0]["type"] == "bar"
    assert "rainfall" in result["summary"].lower()


def test_extreme_analysis_returns_three_counts():
    city_df = _sample_city_df("Sydney")

    result = build_analysis(city_df, "extreme", "Sydney")
    counts = result["chart"]["data"][0]["y"]

    assert len(counts) == 3
    assert all(isinstance(item, int) for item in counts)
