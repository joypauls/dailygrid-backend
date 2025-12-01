# from pprint import pprint
import pandas as pd
from typing import Tuple
import logging
import os

from dailygrid_backend.data_fetcher import get_latest_seven_day_energy_mix
from dailygrid_backend.data_writer import write_json
from dailygrid_backend.utils import get_now_central_string

from dailygrid_backend.types import (
    DISPLAY_TYPE_GROUPS,
    FOSSIL_FUEL_TYPES,
    RENEWABLE_TYPES,
    VALID_GENERATION_TYPES,
    VALID_TYPE_GROUPS,
    type_to_col_name,
)

logging.basicConfig(
    level=logging.INFO,
    format="| %(asctime)s | %(name)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


def add_renewables(df: pd.DataFrame) -> pd.DataFrame:
    # filter by renewables, group by columns, and sum the values
    renewables_df = df[df["type_name"].isin(RENEWABLE_TYPES)]
    renewables_df = (
        renewables_df.groupby(
            [
                "period",
                "respondent",
                "respondent_name",
                "timezone",
                "timezone_description",
                "value_units",
            ]
        )
        .agg({"value": "sum"})
        .reset_index()
    )
    renewables_df["fueltype"] = "Renewables"
    renewables_df["type_name"] = "Renewables"
    return renewables_df


def add_fossil_fuels(df: pd.DataFrame) -> pd.DataFrame:
    # filter by renewables, group by columns, and sum the values
    ff_df = df[df["type_name"].isin(FOSSIL_FUEL_TYPES)]
    ff_df = (
        ff_df.groupby(
            [
                "period",
                "respondent",
                "respondent_name",
                "timezone",
                "timezone_description",
                "value_units",
            ]
        )
        .agg({"value": "sum"})
        .reset_index()
    )
    ff_df["fueltype"] = "Fossil Fuels"
    ff_df["type_name"] = "Fossil Fuels"
    return ff_df


def add_total(df: pd.DataFrame) -> pd.DataFrame:
    # NOTE: excludes other and unknown types
    # filter by renewables, group by columns, and sum the values
    total_df = df[df["type_name"].isin(VALID_GENERATION_TYPES)]
    total_df = (
        total_df.groupby(
            [
                "period",
                "respondent",
                "respondent_name",
                "timezone",
                "timezone_description",
                "value_units",
            ]
        )
        .agg({"value": "sum"})
        .reset_index()
    )
    total_df["fueltype"] = "Total"
    total_df["type_name"] = "Total"
    return total_df


def get_latest_period(df: pd.DataFrame) -> str:
    return df["period"].max()


def get_latest_type_values(
    df: pd.DataFrame, type_name: str, timezone: str = "Central"
) -> Tuple[int, float]:

    latest_period_df = df[
        (df["period"] == get_latest_period(df)) & (df["timezone"] == timezone)
    ]

    # get totals taking into account grouped types
    type_value = 0
    for subtype in VALID_TYPE_GROUPS[type_name]:
        subtype_value = latest_period_df[latest_period_df["type_name"] == subtype][
            "value"
        ].values[0]
        type_value += subtype_value
    total_value = latest_period_df[latest_period_df["type_name"] == "Total"][
        "value"
    ].values[0]

    percent = 0.0
    if total_value > 0:
        percent = round(float(type_value / total_value) * 100, 2)

    return int(type_value), percent


def main():
    """
    Main function to update the data and write to JSON.
    Should be agnostic to local vs deployed environment.
    Used as poetry command in pyproject.toml - make changes with caution (see Makefile).
    """
    logger.info("Running main()")

    # print(now_utc_iso())

    raw_response = get_latest_seven_day_energy_mix()
    # pprint(data)

    # prep
    raw_data = raw_response["data"]
    raw_data_df = pd.DataFrame(raw_data)
    raw_data_df.columns = raw_data_df.columns.str.replace("-", "_", regex=False)
    raw_data_df["value"] = raw_data_df["value"].astype(int)

    # get records and conacetanate
    renewables_df = add_renewables(raw_data_df)
    fossil_fuels_df = add_fossil_fuels(raw_data_df)
    total_df = add_total(raw_data_df)
    processed_data_df = pd.concat(
        [raw_data_df, renewables_df, fossil_fuels_df, total_df]
    )
    processed_data_df = processed_data_df.sort_values(
        by=["period", "fueltype", "type_name"]
    )
    processed_data_df = processed_data_df.reset_index(drop=True)

    # mimic structure of the original data
    processed_data = processed_data_df.to_dict(orient="records")
    raw_response["data"] = processed_data

    # add latest period and values for main visuals
    raw_response["latest"] = {}
    raw_response["latest"]["date"] = get_latest_period(processed_data_df)
    raw_response["latest"]["updated"] = get_now_central_string()
    for type_name in DISPLAY_TYPE_GROUPS:
        value, percent = get_latest_type_values(processed_data_df, type_name)
        raw_response["latest"][type_to_col_name(type_name)] = {
            "megawatthours": value,
            "gigawatthours": int(round(value / 1000, 0)),
            "percent": percent,
            "source": type_name,
        }

    # calculate total for all dates
    all_dates = processed_data_df["period"].unique()
    total_history = []
    for date in all_dates:
        date_total_df = processed_data_df[
            (processed_data_df["period"] == date)
            & (processed_data_df["type_name"] == "Total")
        ]
        total = date_total_df["value"].values[0]
        total_history.append(
            {
                "date": date,
                "megawatthours": int(total),
                "gigawatthours": int(round(total / 1000, 0)),
            }
        )
    raw_response["history"] = {}
    raw_response["history"]["total"] = total_history

    # pprint(raw_response["latest"])
    # print(add_total(raw_records_df))

    write_json(raw_response)


def _main_dev():
    """
    Main function to run locally for testing.
    """
    pass


if __name__ == "__main__":
    # check if this is a local testing run
    if os.getenv("DEV_RUN") == "true":
        logger.info("This is a dev run")
        _main_dev()
    else:
        main()
