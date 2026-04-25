from io import StringIO

from phase9_webapp.csv_data import filter_by_city, get_available_cities, load_weather_data


def test_load_weather_data_uses_location_as_city():
    csv_text = """Location,Date,MaxTemp,Rainfall\nSydney,2024-01-01,30,2\nMelbourne,2024-01-02,25,0\n"""
    df = load_weather_data(StringIO(csv_text))

    assert "City" in df.columns
    assert df["City"].tolist() == ["Sydney", "Melbourne"]


def test_get_available_cities_returns_sorted_unique_values():
    csv_text = """Location,Date,MaxTemp\nBrisbane,2024-01-01,31\nAdelaide,2024-01-02,29\nBrisbane,2024-01-03,33\n"""
    df = load_weather_data(StringIO(csv_text))

    assert get_available_cities(df) == ["Adelaide", "Brisbane"]


def test_filter_by_city_returns_only_selected_city():
    csv_text = """Location,Date,MaxTemp\nPerth,2024-01-01,35\nPerth,2024-01-02,36\nDarwin,2024-01-03,34\n"""
    df = load_weather_data(StringIO(csv_text))

    perth_df = filter_by_city(df, "Perth")
    assert len(perth_df) == 2
    assert perth_df["City"].nunique() == 1
