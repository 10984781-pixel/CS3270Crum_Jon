"""Weather dashboard analysis logic."""

from __future__ import annotations

import pandas as pd


def _monthly_group(city_df):
    if "Date" in city_df.columns and city_df["Date"].notna().any():
        grouped = city_df.dropna(subset=["Date"]).copy()
        grouped["Date"] = pd.to_datetime(grouped["Date"], errors="coerce")
        grouped["Month"] = grouped["Date"].dt.to_period("M").astype(str)
        return grouped.groupby("Month", as_index=False)

    fallback = city_df.copy()
    fallback.loc[:, "Month"] = [f"Row {index + 1}" for index in range(len(fallback))]
    return fallback.groupby("Month", as_index=False)


def _temperature_analysis(city_df, city: str):
    if "MaxTemp" in city_df.columns and city_df["MaxTemp"].notna().any():
        grouped = _monthly_group(city_df)
        plot_df = grouped["MaxTemp"].mean().fillna(0)
        chart = {
            "data": [
                {
                    "x": plot_df["Month"].tolist(),
                    "y": plot_df["MaxTemp"].round(2).tolist(),
                    "type": "scatter",
                    "mode": "lines+markers",
                    "name": "Avg Max Temp",
                    "line": {"color": "#1f77b4"},
                }
            ],
            "layout": {
                "title": f"Temperature Trend - {city}",
                "xaxis": {"title": "Month"},
                "yaxis": {"title": "Temperature (C)"},
            },
        }
        average_temp = city_df["MaxTemp"].mean()
        summary = f"Average maximum temperature for {city} is {average_temp:.1f} C."
        return chart, summary

    chart = {"data": [], "layout": {"title": f"Temperature Trend - {city}"}}
    summary = f"No MaxTemp data available for {city}."
    return chart, summary


def _rainfall_analysis(city_df, city: str):
    if "Rainfall" in city_df.columns and city_df["Rainfall"].notna().any():
        grouped = _monthly_group(city_df)
        plot_df = grouped["Rainfall"].sum().fillna(0)
        chart = {
            "data": [
                {
                    "x": plot_df["Month"].tolist(),
                    "y": plot_df["Rainfall"].round(2).tolist(),
                    "type": "bar",
                    "name": "Total Rainfall",
                    "marker": {"color": "#2ca02c"},
                }
            ],
            "layout": {
                "title": f"Rainfall Pattern - {city}",
                "xaxis": {"title": "Month"},
                "yaxis": {"title": "Rainfall (mm)"},
            },
        }
        total_rain = city_df["Rainfall"].sum()
        summary = f"Total rainfall for {city} is {total_rain:.1f} mm."
        return chart, summary

    chart = {"data": [], "layout": {"title": f"Rainfall Pattern - {city}"}}
    summary = f"No Rainfall data available for {city}."
    return chart, summary


def _extreme_analysis(city_df, city: str):
    hot_days = int((city_df["MaxTemp"] > 35).sum()) if "MaxTemp" in city_df.columns else 0
    heavy_rain_days = int((city_df["Rainfall"] > 20).sum()) if "Rainfall" in city_df.columns else 0
    cold_days = int((city_df["MinTemp"] < 5).sum()) if "MinTemp" in city_df.columns else 0

    chart = {
        "data": [
            {
                "x": ["Hot Days (>35C)", "Heavy Rain Days (>20mm)", "Cold Days (<5C)"],
                "y": [hot_days, heavy_rain_days, cold_days],
                "type": "bar",
                "marker": {"color": ["#d62728", "#17becf", "#9467bd"]},
            }
        ],
        "layout": {
            "title": f"Extreme Weather Indicators - {city}",
            "yaxis": {"title": "Days"},
        },
    }
    summary = (
        f"{city} recorded {hot_days} hot days, {heavy_rain_days} heavy rain days, "
        f"and {cold_days} cold days."
    )
    return chart, summary


def build_analysis(city_df, category: str, city: str):
    if city_df.empty:
        return {
            "chart": {"data": [], "layout": {"title": "No Data"}},
            "summary": f"No records found for {city}.",
        }

    if category == "temperature":
        chart, summary = _temperature_analysis(city_df, city)
    elif category == "rainfall":
        chart, summary = _rainfall_analysis(city_df, city)
    else:
        chart, summary = _extreme_analysis(city_df, city)

    return {"chart": chart, "summary": summary}
