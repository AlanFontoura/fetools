"""
This module provides functions to create a structure for
Partial Ownership and SMA setup
"""

import pandas as pd
from dataclasses import dataclass
from typing import Any


@dataclass
class ColumnConfig:
    source: str = ""
    name: str = ""
    prefix: str = ""
    suffix: str = ""
    value: Any = None


@dataclass
class LoaderConfig:
    name: str = ""
    folder: str = ""
    columns: list[ColumnConfig] = []


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
