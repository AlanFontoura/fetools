import os
from pathlib import Path
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
            df = self.adjust_last_date(df)
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

    def adjust_last_date(self, df: pd.DataFrame) -> pd.DataFrame:
        last_date = pd.to_datetime(
            self.config.get("base", {}).get("stitching_date", "9999-12-31")
        ) - pd.Timedelta(days=1)
        last_date = last_date.strftime("%Y-%m-%d")
        df.loc[
            df["date"]
            == self.config.get("base", {}).get(
                "stitching_date", "9999-12-31"
            ),
            "date",
        ] = last_date
        return df

    def create_output_dir(self):
        client = self.config.get("base", {}).get("client", {})
        output_dir = Path(f"data/outputs/vnf/{client}")
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(Path(output_dir, "inputs"), exist_ok=True)
        os.makedirs(Path(output_dir, "portfolios"), exist_ok=True)
        os.makedirs(Path(output_dir, "bookvalues"), exist_ok=True)
        return output_dir

    def main(self):
        # File creation
        inputs = Inputs(self.df).create_inputs_file()
        portfolios = Portfolios(self.df).create_portfolios_file()
        bookvalues = BookValues(self.df).create_book_values_file()
        misc = MiscFiles(
            self.df, self.config.get("base", {}).get("stitching_date", "")
        )
        historical_config, present_config = (
            misc.create_portfolio_configurations_file()
        )
        offset_transactions = misc.create_offset_transactions()

        # Output directory setup
        output_dir = self.create_output_dir()

        # Save files
        self.create_household_mapping(self.df).to_csv(
            Path(output_dir, "HouseholdMapping.csv"), index=False
        )
        historical_config.to_csv(
            Path(output_dir, "PortfolioConfigs_Historical.csv"), index=False
        )
        present_config.to_csv(
            Path(output_dir, "PortfolioConfigs_Present.csv"), index=False
        )
        offset_transactions.to_csv(
            Path(output_dir, "OffsetTransactions.csv"), index=False
        )

        for idx in inputs["hh_index"].unique():
            inputs_i = inputs[inputs["hh_index"] == idx].drop(
                columns=["hh_index"]
            )
            inputs_i.to_csv(
                Path(output_dir, "inputs", f"own-analytics-set-{idx}.csv"),
                index=False,
            )

            portfolios_i = portfolios[portfolios["hh_index"] == idx].drop(
                columns=["hh_index"]
            )
            portfolios_i.to_csv(
                Path(output_dir, "portfolios", f"portfolio-set-{idx}.csv"),
                index=False,
            )

            bookvalues_i = bookvalues[bookvalues["hh_index"] == idx].drop(
                columns=["hh_index"]
            )
            os.makedirs(
                Path(output_dir, "bookvalues", f"bv-set-{idx}"), exist_ok=True
            )
            bookvalues_i.to_csv(
                Path(
                    output_dir,
                    "bookvalues",
                    f"bv-set-{idx}/bv-set-{idx}_USD.csv",
                ),
                index=False,
            )


class Inputs:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def create_inputs_file(self):
        df = self.add_cash_from_trades(self.df)
        df = self.add_and_rename_columns(df)
        df = self.add_currency_rows(df)
        return df

    def add_cash_from_trades(self, df: pd.DataFrame) -> pd.DataFrame:
        df["previous_market_value"] = df.groupby("account_id")[
            "market_value"
        ].shift(1)
        df["cash_from_trades"] = (
            df["market_value"]
            - df["previous_market_value"] * (1 + df["returns"])
            - df["fin_transfer_in"]
            - df["opr_transfer"]
        )
        # For first entries where previous_market_value is NaN, set cash_from_trades to 0
        df.loc[df["previous_market_value"].isna(), "cash_from_trades"] = 0
        df = df.drop(columns=["previous_market_value"])

        return df

    def add_and_rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        inputs = pd.DataFrame()
        inputs["Date"] = df["date"]
        inputs["Portfolio Firm Provided Key"] = df["account_id"]
        inputs["Position Firm Provided Key"] = "legacy_instrument_USD"
        inputs["Currency Split Type"] = "0"
        inputs["Value"] = df["market_value"]
        inputs["Quantity"] = df["market_value"]
        inputs["NumUnits"] = df["market_value"]
        inputs["DateCashFrTrades"] = df["cash_from_trades"]
        inputs["DateFees"] = df["fees"]
        inputs["DateExpenses"] = df["expenses"]
        inputs["DateCashTransfer"] = 0
        inputs["DateCashTransferIn"] = 0
        inputs["DateTransferredPosVal"] = 0
        inputs["DateTransferredInPosVal"] = 0
        inputs["DateCashInternalTransfer"] = 0
        inputs["DateCashInternalTransferIn"] = 0
        inputs["hh_index"] = df["hh_index"]
        return inputs

    def add_currency_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        compl = df.copy()
        compl["Position Firm Provided Key"] = "USD"
        compl["Currency Split Type"] = "COMPL"
        compl[
            [
                "Value",
                "Quantity",
                "NumUnits",
                "DateFees",
                "DateExpenses",
                "DateCashFrTrades",
            ]
        ] = 0
        main = compl.copy()
        main["Currency Split Type"] = "MAIN"
        final_df = pd.concat([df, compl, main], ignore_index=True)
        final_df = final_df.sort_values(
            by=["Portfolio Firm Provided Key", "Date", "Currency Split Type"]
        ).reset_index(drop=True)
        return final_df


class Portfolios:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def create_portfolios_file(self):
        portfolios = (
            self.df[["account_id", "hh_index"]]
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
        bookvalues = pd.DataFrame()
        bookvalues["PortfolioID"] = self.df["account_id"]
        bookvalues["Date"] = self.df["date"]
        bookvalues["InstrumentID"] = "legacy_instrument_USD"
        bookvalues["CurrencySplitType"] = "0"
        bookvalues["DateOprTransfPosVal"] = self.df["opr_transfer"]
        bookvalues["DateFinTransfPosVal"] = self.df["fin_transfer"]
        bookvalues["DateFinTransfInPosVal"] = self.df["fin_transfer_in"]
        bookvalues[
            [
                "DateTradeAmt",
                "BookNumUnits",
                "BookValue",
                "DateTransferredCost",
                "DateRealizedPnl",
                "InternalBookNumUnits",
                "InternalBookValue",
                "DateInternalTransferredCost",
                "DateInternalRealizedPnl",
                "SettledBookValue",
            ]
        ] = 0
        bookvalues["hh_index"] = self.df["hh_index"]
        return bookvalues


class MiscFiles:
    def __init__(self, df: pd.DataFrame, stitching_date: str):
        self.df = df
        self.stitching_date = stitching_date

    def create_portfolio_configurations_file(
        self,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        historical = (
            self.df[["account_id", "date"]]
            .copy()
            .drop_duplicates("account_id", keep="first")
            .reset_index(drop=True)
            .rename(
                columns={"account_id": "CustodianAccountID", "date": "Date"}
            )
        )
        historical["SleeveID"] = [
            f"{acc}_PrimarySleeve" for acc in historical["CustodianAccountID"]
        ]
        historical["Portfolio In Terms Of"] = "Transactions"
        historical["Tracking Type"] = "OwnAnalytics"
        historical["Are Splits Per Position"] = False
        historical["Are Cashflows Per Position"] = True

        present = historical.copy()
        present["Date"] = self.stitching_date
        present["Tracking Type"] = "Transactions"

        return historical, present

    def create_offset_transactions(self) -> pd.DataFrame:
        offset = self.df.copy()
        last_date = offset["date"].max()
        offset = offset.loc[
            (offset["date"] == last_date) & (offset["market_value"] != 0),
            ["account_id", "market_value"],
        ]
        offset = offset.rename(
            columns={
                "account_id": "Custodian Account ID",
                "market_value": "Amount",
            }
        )
        offset["Type"] = [
            (
                "Transfer Security Out"
                if amount > 0
                else "Transfer Security In"
            )
            for amount in offset["Amount"]
        ]
        offset["Amount"] = abs(offset["Amount"])
        offset["Quantity"] = offset["Amount"]
        offset["Market Value in Transaction Currency"] = offset["Amount"]
        offset["Process Date"] = self.stitching_date
        offset["Settle Date"] = self.stitching_date
        offset["Trade Date"] = self.stitching_date
        offset["Currency Name"] = "USD"
        offset["Instrument ID"] = "legacy_instrument_USD"
        offset["Transaction ID"] = [
            f"vnf_transfer_{account}"
            for account in offset["Custodian Account ID"]
        ]
        return offset


if __name__ == "__main__":
    vnf = ValuesAndFlows("data/inputs/vnf/gresham/vnf.toml")
    vnf.main()
