import pandas as pd
import tomllib
from typing import Any


def load_vnf_config(toml_file_path: str) -> dict[str, Any]:
    with open(toml_file_path, "rb") as file:
        config_data = tomllib.load(file)

    return config_data


class ValuesAndFlows:
    def __init__(self, config_file_path: str):
        self.config: dict[str, Any] = load_vnf_config(config_file_path)
        self._df: pd.DataFrame | None = None

    @property
    def df(self) -> pd.DataFrame:
        if self._df is None:
            df = pd.read_csv(self.config["base"]["data"])
            df = self.modify_dataframe(df)
            household_mapping = self.create_household_mapping(df)
            df = df.merge(household_mapping, on="household_id", how="left")
            df = self.add_transfers_in(df)
            df = self.add_zero_entries_for_closed_accounts(df)
            self._df = df
        return self._df

    def modify_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        for new_col, current_col in self.config.get("columns", {}).items():
            df[new_col] = df[current_col] if current_col else 0
        df = df[self.config.get("columns", {}).keys()]
        df = df[
            df["date"]
            <= self.config.get("base", {}).get("stitching_date", "9999-12-31")
        ]
        df = df.sort_values(
            by=["household_id", "account_id", "date"]
        ).reset_index(drop=True)
        return df

    def create_household_mapping(self, df: pd.DataFrame) -> pd.DataFrame:
        household_mapping = (
            df[["household_id"]].drop_duplicates().reset_index(drop=True)
        )
        household_mapping["hh_index"] = household_mapping.index
        return household_mapping

    def add_transfers_in(self, df: pd.DataFrame) -> pd.DataFrame:
        first_rows = df.groupby("account_id").head(1).index
        df.loc[first_rows, "opr_transfer"] = (
            df.loc[first_rows, "market_value"]
            - df.loc[first_rows, "fin_transfer"]
        )

        return df

    def add_zero_entries_for_closed_accounts(
        self, df: pd.DataFrame
    ) -> pd.DataFrame:
        df["date"] = pd.to_datetime(df["date"])
        last_entries = df.groupby("account_id").tail(1).copy()
        last_entries["date"] = last_entries["date"] + pd.offsets.MonthEnd(1)
        last_entries["opr_transfer"] = -1 * last_entries["market_value"]
        last_entries[
            [
                "market_value",
                "fin_transfer",
                "fin_transfer_in",
                "fees",
                "expenses",
                "returns",
            ]
        ] = 0
        df = pd.concat([df, last_entries], ignore_index=True)
        df = df[
            df["date"]
            <= self.config.get("base", {}).get("stitching_date", "9999-12-31")
        ]
        df = df.sort_values(
            by=["hh_index", "account_id", "date"]
        ).reset_index(drop=True)

        return df


class Inputs:
    def __init__(self, df: pd.DataFrame):
        self.df = df


class Portfolios:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def create_portfolios_file(self):
        portfolios = (
            self.df[["account_id", "index"]]
            .drop_duplicates()
            .reset_index(drop=True)
        )
        portfolios["account_id"] = [
            f"{acc}_PrimarySleeve" for acc in portfolios["account_id"]
        ]
        portfolios = portfolios.rename(
            columns={"account_id": "Firm Provided Key"}
        )
        return portfolios


class BookValues:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def create_book_values_file(self):
        pass


if __name__ == "__main__":
    vnf = ValuesAndFlows("data/inputs/vnf/gresham/vnf.toml")
    print(vnf.df.head(20))
    print(vnf.df.shape[0])
