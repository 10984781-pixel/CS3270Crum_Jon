"""CSV ingestion and filtering logic."""

from __future__ import annotations

import pandas as pd

NUMERIC_COLUMNS = [
    "MinTemp",
    "MaxTemp",
    "Rainfall",
    "WindGustSpeed",
    "Humidity9am",
    "Humidity3pm",
]
CITY_COLUMN_OPTIONS = ["Location", "City", "Town"]
DATE_COLUMN_OPTIONS = ["Date", "date"]


def _find_column(dataframe, options):
    lowered = {col.lower(): col for col in dataframe.columns}
    for name in options:
        actual = lowered.get(name.lower())
        if actual:
            return actual
    return None


def load_weather_data(source):
    dataframe = pd.read_csv(source)
    dataframe.columns = [col.strip() for col in dataframe.columns]

    city_col = _find_column(dataframe, CITY_COLUMN_OPTIONS)
    if city_col is None:
        raise ValueError("CSV must include a city column (Location/City/Town).")

    date_col = _find_column(dataframe, DATE_COLUMN_OPTIONS)

    if city_col != "City":
        dataframe = dataframe.rename(columns={city_col: "City"})
    if date_col and date_col != "Date":
        dataframe = dataframe.rename(columns={date_col: "Date"})

    dataframe = dataframe.copy()
    dataframe["City"] = dataframe["City"].astype(str).str.strip()

    if "Date" in dataframe.columns:
        dataframe["Date"] = pd.to_datetime(dataframe["Date"], errors="coerce")

    for column in NUMERIC_COLUMNS:
        if column in dataframe.columns:
            dataframe[column] = pd.to_numeric(dataframe[column], errors="coerce")

    dataframe = dataframe.dropna(subset=["City"])
    dataframe = dataframe[dataframe["City"] != ""]

    if dataframe.empty:
        raise ValueError("CSV does not contain usable rows.")

    return dataframe


def get_available_cities(dataframe) -> list[str]:
    return sorted(city for city in dataframe["City"].dropna().unique() if str(city).strip())


def filter_by_city(dataframe, city: str):
    return dataframe[dataframe["City"] == city].copy()
