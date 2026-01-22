"""
Tools to create VnF importers
"""

from typing import Any
import pandas as pd
import tomllib
from pathlib import Path
from datetime import datetime, timedelta
from dataclasses import dataclass
from pprint import pprint
import os


@dataclass
class ColumnConfig:
    column_name: str | float | None = None
    value: str | float | None = None
    source_column: str | float | None = None


# region Base data functions
def get_base_data(configs: dict[str, Any]) -> pd.DataFrame:
    df = read_data(configs)
    df = rename_data(df, configs)
    df = filter_data(df, configs)
    df = df.sort_values(by=["Account ID", "Date"]).reset_index(drop=True)
    return df


def read_data(configs: dict[str, Any]) -> pd.DataFrame:
    if str(configs.get("base_data")).startswith("s3"):
        df = pd.read_csv(str(configs.get("base_data")))
        return df
    file_path = Path(
        "data",
        "inputs",
        "vnf",
        configs["client"],
        configs["base_data"],
    )
    if str(file_path).endswith(".csv"):
        df = pd.read_csv(str(file_path))
    if str(file_path).endswith(".parquet"):
        df = pd.read_parquet(str(file_path))
    return df


def rename_data(df: pd.DataFrame, configs: dict[str, Any]) -> pd.DataFrame:
    mapping = configs.get("column_mappings", {})
    if not mapping:
        return df
    for new_col, old_col in mapping.items():
        df.rename(columns={old_col: new_col}, inplace=True)
    return df


def filter_data(df: pd.DataFrame, configs: dict[str, Any]) -> pd.DataFrame:
    stitch_date = configs.get("stitch_date", None)
    if not stitch_date:
        return df
    df = df[df["Date"] <= stitch_date].reset_index(drop=True)
    return df


# endregion


# region Configuration creation functions
def create_file_configs(configs: list[dict[str, Any]]) -> list[ColumnConfig]:
    return [create_column_config(config) for config in configs]


def create_column_config(
    config: dict[str, str | float | None],
) -> ColumnConfig:
    return ColumnConfig(
        column_name=config.get("column_name"),
        value=config.get("value", None),
        source_column=config.get("source_column", None),
    )


# endregion


# region DataFrame modification functions
def modify_dataframe(
    df: pd.DataFrame,
    configs: list[ColumnConfig],
) -> pd.DataFrame:
    cols = [config.column_name for config in configs]
    for config in configs:
        df = modify_column(df, config)
    df = df[cols].drop_duplicates().reset_index(drop=True)
    return df


def modify_column(
    df: pd.DataFrame,
    column_config: ColumnConfig,
) -> pd.DataFrame:
    if column_config.value is not None:
        df[column_config.column_name] = column_config.value
    elif column_config.source_column is not None:
        df[column_config.column_name] = df[column_config.source_column]
    return df


def copy_column(
    df: pd.DataFrame, column_name: str, source_column: str
) -> pd.DataFrame:
    df[column_name] = df[source_column]
    return df


def create_column(
    df: pd.DataFrame, column_name: str, default_value: Any
) -> pd.DataFrame:
    df[column_name] = default_value
    return df


# endregion


# Create input files
## Add net inflow when account is opened
## Add zero entries when account is closed
## Add net cashflow when account is closed
## Calculate DateCashFrTrades based on provided returns
## Add COMPL and MAIN rows

# Create portfolio files
# Create portfolio configurations

# Create transfer out transactions


# Sort data frame
# Filter by stitch date
# Adjust last date
# Save data
def save_all_files(
    inputs: pd.DataFrame,
    portfolios: pd.DataFrame,
    bookvalues: pd.DataFrame,
    configs: dict[str, Any],
) -> None:
    number_of_households = len(
        get_household_mapping(configs, "Portfolio Firm Provided Key")[
            "Household ID"
        ].unique()
    )
    create_folder_structure(configs, number_of_households)
    save_inputs(inputs, configs)
    save_portfolios(portfolios, configs)
    currency_list = configs.get("currency_list", ["USD"])
    for currency in currency_list:
        save_bookvalues(bookvalues, configs, currency)


def save_inputs(df: pd.DataFrame, configs: dict[str, Any]) -> None:
    output_folder = get_output_folder(configs)
    output_path = Path(output_folder, "inputs")
    household_mapping = get_household_mapping(
        configs, "Portfolio Firm Provided Key"
    )
    df = df.merge(
        household_mapping, how="left", on="Portfolio Firm Provided Key"
    )
    households = df["Household ID"].unique().tolist()
    for index, household in enumerate(households):
        household_df = df[df["Household ID"] == household].drop(
            columns=["Household ID"]
        )
        save_data(
            household_df,
            output_path,
            "own-analytics-set",
            index,
        )


def save_portfolios(df: pd.DataFrame, configs: dict[str, Any]) -> None:
    output_folder = get_output_folder(configs)
    output_path = Path(output_folder, "portfolios")
    household_mapping = get_household_mapping(configs, "Firm Provided Key")
    df = df.merge(household_mapping, how="left", on="Firm Provided Key")
    households = df["Household ID"].unique().tolist()
    for index, household in enumerate(households):
        household_df = df[df["Household ID"] == household].drop(
            columns=["Household ID"]
        )
        save_data(household_df, output_path, "portfolio-set", index)


def save_bookvalues(
    df: pd.DataFrame, configs: dict[str, Any], currency: str
) -> None:
    output_folder = get_output_folder(configs)
    output_path = Path(output_folder, "bookvalues")
    household_mapping = get_household_mapping(configs, "PortfolioID")
    df = df.merge(household_mapping, how="left", on="PortfolioID")
    households = df["Household ID"].unique().tolist()
    for index, household in enumerate(households):
        household_df = df[df["Household ID"] == household].drop(
            columns=["Household ID"]
        )
        save_data(
            household_df,
            Path(output_path, f"bv-set-{index}"),
            "bv-set",
            index,
        )


def save_data(
    df: pd.DataFrame,
    folder: Path,
    filename: str,
    index: int,
    currency: str | None = None,
) -> None:
    output_file = Path(folder, f"{filename}-{index}.csv")
    if currency:
        output_file = Path(folder, f"{filename}-{index}_{currency}.csv")
    df.to_csv(output_file, index=False)


def create_folder_structure(
    configs: dict[str, Any], number_of_households: int
) -> None:
    output_folder = get_output_folder(configs)
    if output_folder.startswith("s3"):
        return
    os.makedirs(output_folder, exist_ok=True)
    os.makedirs(Path(output_folder, "inputs"), exist_ok=True)
    os.makedirs(Path(output_folder, "portfolios"), exist_ok=True)
    for i in range(number_of_households):
        os.makedirs(
            Path(output_folder, "bookvalues", f"bv-set-{i}"), exist_ok=True
        )


def get_output_folder(configs: dict[str, Any]) -> str:
    client = configs.get("client", "unknown_client")
    output_folder = configs.get("output_folder", None)
    if not output_folder:
        output_path = Path("data", "outputs", "vnf", client)
        return str(output_path)
    return str(output_folder)


def get_household_mapping(
    configs: dict[str, Any], account_col_name: str
) -> pd.DataFrame:
    if str(configs.get("household_mapping")).startswith("s3"):
        df = pd.read_csv(str(configs.get("household_mapping")))
        return df.rename(columns={"Account ID": account_col_name})
    file_path = Path(
        "data",
        "inputs",
        "vnf",
        configs["client"],
        configs["household_mapping"],
    )
    if str(file_path).endswith(".csv"):
        df = pd.read_csv(str(file_path))
    if str(file_path).endswith(".parquet"):
        df = pd.read_parquet(str(file_path))
    return df.rename(columns={"Account ID": account_col_name})


if __name__ == "__main__":
    # Example usage
    with open("data/inputs/vnf/gresham/vnf.toml", "rb") as f:
        config = tomllib.load(f)
    pprint(config)
    df = get_base_data(config)
    inputs = modify_dataframe(df, create_file_configs(config["inputs"]))
    portfolios = modify_dataframe(
        df, create_file_configs(config["portfolios"])
    )
    bookvalues = modify_dataframe(
        df, create_file_configs(config["bookvalues"])
    )
    save_all_files(inputs, portfolios, bookvalues, config)
