"""
Tools to create VnF importers
"""

from typing import Any
import pandas as pd
import tomllib
from pathlib import Path
from datetime import datetime, timedelta


class BaseTransformer:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def create_column(
        self, column_name: str, default_value: Any
    ) -> pd.DataFrame:
        self.df[column_name] = default_value
        return self.df

    def copy_column(
        self, column_name: str, source_column: str
    ) -> pd.DataFrame:
        self.df[column_name] = self.df[source_column]
        return self.df

    def transform(self, configs: list[dict[str, Any]]) -> pd.DataFrame:
        columns = []
        for config in configs:
            columns.append(config["column_name"])
            if "value" in config:
                self.create_column(config["column_name"], config["value"])
            elif "source_column" in config:
                self.copy_column(
                    config["column_name"], config["source_column"]
                )
        return self.df[columns]


# Create input files
class VnFInputFilesCreator(BaseTransformer):
    def __init__(self, df: pd.DataFrame, configs: list[dict[str, Any]]):
        super().__init__(df)
        self.configs = configs

    def create_base_input_files(self) -> pd.DataFrame:
        return self.transform(self.configs)


## Add net inflow when account is opened
## Add zero entries when account is closed
## Add net cashflow when account is closed
## Calculate DateCashFrTrades based on provided returns
## Add COMPL and MAIN rows


# Create portfolio files
class VnFPortfolioFilesCreator(BaseTransformer):
    def __init__(self, df: pd.DataFrame, configs: list[dict[str, Any]]):
        super().__init__(df)
        self.configs = configs

    def create_portfolio_files(self) -> pd.DataFrame:
        return self.transform(self.configs)


# Create bookvalue files
class VnFBookValueFilesCreator(BaseTransformer):
    def __init__(self, df: pd.DataFrame, configs: list[dict[str, Any]]):
        super().__init__(df)
        self.configs = configs

    def create_bookvalue_files(self) -> pd.DataFrame:
        return self.transform(self.configs)

    pass


# Create portfolio configurations
class VnFPortfolioConfigurations:
    pass


# Create transfer out transactions
class VnFTransferOutTransactions:
    pass


class ValuesAndFlowsTools:
    def __init__(self, toml_file: str):
        with open(toml_file, "rb") as f:
            self.config = tomllib.load(f)
        base_data = Path(
            "data",
            "inputs",
            "vnf",
            self.config["client"],
            self.config["base_data"],
        )
        if str(base_data).endswith(".csv"):
            self.df = pd.read_csv(base_data)
        if str(base_data).endswith(".parquet"):
            self.df = pd.read_parquet(base_data)

    @property
    def last_date(self) -> str:
        last_date = datetime.strptime(
            self.config["stitch_date"], "%Y-%m-%d"
        ).date()
        prior_date = last_date - timedelta(days=1)
        prior_date_str = prior_date.strftime("%Y-%m-%d")
        return prior_date_str

    def format_base_data(self) -> None:
        self.df = self.df[self.df["Date"] <= self.config["stitch_date"]]
        self.df = self.df.sort_values(
            by=["Household ID", "Account ID", "Date"]
        )
        self.df.loc[self.df["Date"] == self.config["stitch_date"], "Date"] = (
            self.last_date
        )

    def adjust_account_dates(self) -> pd.DataFrame:
        dates_df = self.df.drop_duplicates(
            subset=["Account ID"], keep="first"
        ).reset_index(drop=True)
        dates_df = dates_df[["Account ID", "Date"]]
        dates_df["Inception Date"] = dates_df["Date"]
        dates_df = dates_df.rename(columns={"Date": "Date Opened"})
        return dates_df

    def create_historical_portfolio_config(self) -> pd.DataFrame:
        historical_portfolio_config = self.df.drop_duplicates(
            subset=["Account ID"], keep="first"
        ).reset_index(drop=True)
        historical_portfolio_config = historical_portfolio_config[
            ["Date", "Account ID"]
        ]
        return self.df

    def main(self) -> None:
        self.format_base_data()
        account_dates_df = self.adjust_account_dates()


# Sort data frame
# Filter by stitch date
# Adjust last date

if __name__ == "__main__":
    # Example usage
    with open("config.toml", "rb") as f:
        config = tomllib.load(f)
    print(config)
