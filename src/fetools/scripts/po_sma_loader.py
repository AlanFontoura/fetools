"""
This module provides functions to create a structure for
Partial Ownership and SMA setup
"""

import pandas as pd
from dataclasses import dataclass, field
from typing import Any, cast


@dataclass(frozen=True)
class ColumnConfig:
    source: str = ""
    name: str = ""
    prefix: str = ""
    suffix: str = ""
    value: Any = None


@dataclass(frozen=True)
class LoaderConfig:
    name: str = ""
    folder: str = ""
    columns: list[ColumnConfig] = field(default_factory=list)


class DataTransformer:
    def __init__(self, df: pd.DataFrame, loader: LoaderConfig):
        self.df = df
        self.loader = loader

    def transform(self) -> pd.DataFrame:
        final_columns = []
        for column_config in self.loader.columns:
            self.df = self.transform_column(column_config)
            final_columns.append(column_config.name)
        self.df = self.df[final_columns]
        return self.df

    def transform_column(self, config: ColumnConfig) -> pd.DataFrame:
        if config.value is not None:
            self.df = self.create_constant_column(config.name, config.value)
        else:
            self.df[config.name] = self.add_prefix_suffix(
                config.prefix, self.df[config.source], config.suffix
            )
        return self.df

    @staticmethod
    def add_prefix_suffix(
        prefix: str, series: pd.Series, suffix: str
    ) -> pd.Series:
        return pd.Series([f"{prefix}{val}{suffix}" for val in series])

    def create_constant_column(
        self, column_name: str, value: Any
    ) -> pd.DataFrame:
        self.df[column_name] = value
        return self.df


def extend_ownership_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extends the ownership DataFrame by adding multi level ownership of ownership
    entries.

    Parameters:
    df (pd.DataFrame): DataFrame containing 'Owner', 'Owned', 'Date' and 'Percentage'
    columns.

    Returns:
    pd.DataFrame: Extended DataFrame with additional multi level ownership of
    ownership entries.
    """
    number_of_entries = df.shape[0]
    keep_going = True
    while keep_going:
        new_entries = simple_ownership_of_ownership(df)
        df = pd.concat([df, new_entries], ignore_index=True).drop_duplicates()
        if df.shape[0] == number_of_entries:
            keep_going = False
        else:
            number_of_entries = df.shape[0]
    return df.sort_values(by=["Owned", "Owner", "Date"]).reset_index(
        drop=True
    )


def simple_ownership_of_ownership(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates a single level of ownership of ownership for each entity in the
    DataFrame.

    Parameters:
    df (pd.DataFrame): DataFrame containing 'Owner', 'Owned', 'Date' and 'Percentage'
    columns.

    Returns:
    pd.DataFrame: DataFrame with additional entries representing one level of
    ownership of ownership.
    """
    ownership_of_ownership = df.merge(
        df, left_on="Owned", right_on="Owner", how="inner"
    )
    ownership_of_ownership["Date"] = [
        max(d1, d2)
        for d1, d2 in zip(
            ownership_of_ownership["Date_x"], ownership_of_ownership["Date_y"]
        )
    ]
    ownership_of_ownership = ownership_of_ownership.sort_values(
        by=["Owned_y", "Owner_x", "Date", "Date_x", "Date_y"]
    ).drop_duplicates(subset=["Owner_x", "Owned_y", "Date"], keep="last")
    ownership_of_ownership["Owner"] = ownership_of_ownership["Owner_x"]
    ownership_of_ownership["Owned"] = ownership_of_ownership["Owned_y"]
    ownership_of_ownership["Percentage"] = (
        ownership_of_ownership["Percentage_x"]
        * ownership_of_ownership["Percentage_y"]
    )
    ownership_of_ownership = ownership_of_ownership[
        ["Owner", "Owned", "Date", "Percentage"]
    ]
    return ownership_of_ownership


def apply_cutoff_date_to_fco(
    df: pd.DataFrame, cutoff_date: str
) -> pd.DataFrame:
    """
    Applies a cutoff date to the ownership DataFrame
    On the cutoff date, only the latest entry prior to or on the cutoff date
    is kept for each Owned-Owner pair. Entries after the cutoff date are kept as is

    Parameters:
    df (pd.DataFrame): DataFrame containing 'Date' column.
    cutoff_date (str format, YYYY-MM-DD): The cutoff date to filter the DataFrame.

    Returns:
    pd.DataFrame: Filtered DataFrame with entries on or after the cutoff date.
    """
    df = df.sort_values(by=["Owned", "Date", "Owner"]).reset_index(drop=True)
    after = df[df["Date"] > cutoff_date]
    prior = df[df["Date"] <= cutoff_date]
    if not prior.empty:
        prior = prior.drop_duplicates(subset=["Owned", "Owner"], keep="last")
        prior["Date"] = cutoff_date
    df = pd.concat([prior, after], ignore_index=True)
    return df.sort_values(by=["Owned", "Date", "Owner"]).reset_index(
        drop=True
    )


def add_zero_entries_to_owner(
    fco: pd.DataFrame = pd.DataFrame(),
) -> pd.DataFrame:
    """
    Adds zero percentage entries to the ownership DataFrame
    for all combinations of Owners and Owned entities that do not exist in the DataFrame.

    Parameters:
    fco (pd.DataFrame): DataFrame containing 'Owner', 'Owned', 'Date' and 'Percentage'
    columns for a single 'Owned' entity.

    Returns:
    pd.DataFrame: DataFrame with added zero percentage entries.
    """
    if fco.empty:
        return fco
    dates = fco["Date"].unique().tolist()
    dates.sort()
    owned = fco["Owned"].iloc[0]
    current_owners = (
        fco.loc[fco["Date"] == dates[0], "Owner"].unique().tolist()
    )
    for date in dates[1:]:
        owners_at_date = (
            fco.loc[fco["Date"] == date, "Owner"].unique().tolist()
        )
        for owner in current_owners:
            if owner not in owners_at_date:
                new_entry = pd.DataFrame(
                    {
                        "Owner": [owner],
                        "Owned": [owned],
                        "Date": [date],
                        "Percentage": [0.0],
                    }
                )
                fco = cast(
                    pd.DataFrame,
                    pd.concat([fco, new_entry], ignore_index=True),
                )
        current_owners = (
            fco.loc[fco["Date"] == date, "Owner"].unique().tolist()
        )
    return fco.sort_values(by=["Date", "Owner"]).reset_index(drop=True)


def add_zero_entries_to_fco(
    df: pd.DataFrame = pd.DataFrame(),
) -> pd.DataFrame:
    """
    Adds zero percentage entries to the ownership DataFrame
    for all combinations of Owners and Owned entities that do not exist in the DataFrame.

    Parameters:
    df (pd.DataFrame): DataFrame containing 'Owner', 'Owned', 'Date' and 'Percentage'
    columns.

    Returns:
    pd.DataFrame: DataFrame with added zero percentage entries.
    """
    if df.empty:
        return df
    fco_groups = []
    owned_entities = df["Owned"].unique().tolist()
    for owned in owned_entities:
        fco_owned = df[df["Owned"] == owned].reset_index(drop=True)
        fco_owned_extended = add_zero_entries_to_owner(fco_owned)
        fco_groups.append(fco_owned_extended)
    return (
        pd.concat(fco_groups, ignore_index=True)
        .sort_values(by=["Owned", "Date", "Owner"])
        .reset_index(drop=True)
    )
