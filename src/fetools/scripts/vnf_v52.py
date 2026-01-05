import pandas as pd
import polars as pl
import importlib
from pathlib import Path
from datetime import datetime


class VnFV52:
    """
    Preprocess VnF data from a CSV file.

    Args:
        file_path (str): The path to the CSV file containing VnF data.
    """

    def __init__(self, client: str):
        today = datetime.today().strftime("%Y-%m-%d")
        self.client = client
        self.output_path = f"./outputs/vnf_v52/{client}/{today}"
        self._client_data = None
        self.date_format = "%Y-%m-%d"

    @property
    def client_data(self) -> pl.DataFrame:
        if self._client_data is None:
            try:
                preprocessor = importlib.import_module(
                    f"preprocess.vnf.{self.client.lower()}"
                )
            except ModuleNotFoundError:
                print(
                    f"❌ Preprocessor for client '{client_name}' not found. Check the 'preprocess/vnf' folder."
                )
                sys.exit(1)

            client_preprocessor = preprocessor.PreProcessVnFData(
                file_path="s3://d1g1t-production-file-transfer-us-east-1/gresham/for_d1g1t/PnL/2025-10-10/SecurityLevel/"
            )
            self._client_data = client_preprocessor.pre_process()

        return self._client_data

    @property
    def household_mapping(self):
        return (
            self.client_data.select(["Account ID", "Household ID"])
            .unique()
            .sort("Account ID")
        )

    @property
    def households(self):
        return (
            self.household_mapping.select(["Household ID"])
            .unique()
            .to_series()
            .sort()
            .to_list()
        )

    @property
    def max_date(self):
        return self.client_data["Date"].max()

    @property
    def main_sec_instruments(self):
        main_sec_instruments = self.client_data.select(
            ["Date", "Account ID", "Security ID", "Value", "NumUnits"]
        )
        main_sec_instruments = main_sec_instruments.filter(
            pl.col("Security ID") != "USD"
        )
        main_sec_instruments = main_sec_instruments.with_columns(
            pl.col("NumUnits").alias("Quantity"),
            pl.lit("0").alias("Currency Split Type"),
        )
        return main_sec_instruments

    @property
    def main_cash(self):
        main_sec_instruments = self.client_data.select(
            ["Date", "Account ID", "Security ID", "Value", "NumUnits"]
        )
        main_sec_instruments = main_sec_instruments.filter(
            pl.col("Security ID") == "USD"
        )
        main_sec_instruments = main_sec_instruments.with_columns(
            pl.col("NumUnits").alias("Quantity"),
            pl.lit("MAIN").alias("Currency Split Type"),
        )
        return main_sec_instruments

    @property
    def main_values(self):
        return pl.concat([self.main_sec_instruments, self.main_cash], how="diagonal")

    @property
    def date_cash_from_trades(self):
        date_cash_from_trades = self.client_data.select(
            ["Date", "Account ID", "Security ID", "DateCashFrTrades"]
        )
        date_cash_from_trades = date_cash_from_trades.filter(
            pl.col("DateCashFrTrades") != 0
        )
        date_cash_from_trades = date_cash_from_trades.with_columns(
            pl.lit("0").alias("Currency Split Type")
        )
        return date_cash_from_trades

    @property
    def date_cash_from_trades_offset(self):
        date_cash_from_trades = self.client_data.select(
            ["Date", "Account ID", "DateCashFrTrades"]
        )
        date_cash_from_trades = date_cash_from_trades.filter(
            pl.col("DateCashFrTrades") != 0
        )
        date_cash_from_trades_offset = date_cash_from_trades.group_by(
            ["Date", "Account ID"]
        ).agg((pl.col("DateCashFrTrades").sum() * -1).alias("DateCashFrTrades"))
        date_cash_from_trades_offset = date_cash_from_trades_offset.with_columns(
            pl.lit("USD").alias("Security ID"),
            pl.lit("COMPL").alias("Currency Split Type"),
        )
        return date_cash_from_trades_offset

    @property
    def fees_and_expenses(self):
        fees_and_expenses = self.client_data.select(
            ["Date", "Account ID", "DateFees", "DateExpenses"]
        )
        fees_and_expenses = fees_and_expenses.filter(
            (pl.col("DateFees") != 0) | (pl.col("DateExpenses") != 0)
        )
        fees_and_expenses = fees_and_expenses.group_by(["Date", "Account ID"]).agg(
            pl.col("DateFees").sum().alias("DateFees"),
            pl.col("DateExpenses").sum().alias("DateExpenses"),
        )
        fees_and_expenses = fees_and_expenses.with_columns(
            pl.lit("USD").alias("Security ID"),
            pl.lit("COMPL").alias("Currency Split Type"),
        )
        return fees_and_expenses

    @property
    def inputs(self):
        inputs = pl.concat(
            [
                self.main_values,
                self.date_cash_from_trades,
                self.date_cash_from_trades_offset,
                self.fees_and_expenses,
            ],
            how="diagonal",
        )
        inputs = inputs.fill_null(0)
        inputs = inputs.group_by(
            ["Date", "Account ID", "Security ID", "Currency Split Type"]
        ).agg(pl.all().sum())
        inputs = inputs.with_columns(
            pl.lit(0).alias("DateCashTransfer"),
            pl.lit(0).alias("DateCashTransferIn"),
            pl.lit(0).alias("DateTransferredPosVal"),
            pl.lit(0).alias("DateTransferredInPosVal"),
            pl.lit(0).alias("DateCashInternalTransfer"),
            pl.lit(0).alias("DateCashInternalTransferIn"),
        )
        inputs = self.add_zero_entries(inputs)
        inputs = self.add_usd_entries(inputs)
        inputs = self.adjust_last_date(inputs)
        inputs = inputs.sort(
            by=["Account ID", "Date", "Currency Split Type", "Security ID"]
        ).rename(
            {
                "Account ID": "Portfolio Firm Provided Key",
                "Security ID": "Position Firm Provided Key",
            }
        )
        return inputs

    def add_zero_entries(self, df: pl.DataFrame) -> pl.DataFrame:
        all_dates = df.select(
            ["Account ID", "Currency Split Type", "Security ID", "Date"]
        ).unique()
        all_dates = all_dates.filter(pl.col("Date") < self.max_date)
        all_dates = all_dates.with_columns(
            (
                pl.col("Date")
                .str.strptime(pl.Date, self.date_format)
                .dt.offset_by("1d")
                .dt.month_end()
                .dt.strftime(self.date_format)
            ).alias("Date")
        )
        df = (
            pl.concat([df, all_dates], how="diagonal")
            .fill_null(0)
            .group_by(["Date", "Account ID", "Security ID", "Currency Split Type"])
            .agg(pl.all().sum())
            .sort(by=["Account ID", "Date", "Currency Split Type", "Security ID"])
        )
        return df

    def add_usd_entries(self, df: pl.DataFrame) -> pl.DataFrame:
        main = df.select(["Date", "Account ID"]).unique()
        main = main.with_columns(
            pl.lit("USD").alias("Security ID"),
            pl.lit("MAIN").alias("Currency Split Type"),
        )

        compl = df.select(["Date", "Account ID"]).unique()
        compl = compl.with_columns(
            pl.lit("USD").alias("Security ID"),
            pl.lit("COMPL").alias("Currency Split Type"),
        )

        df = (
            pl.concat([df, main, compl], how="diagonal")
            .group_by(["Date", "Account ID", "Security ID", "Currency Split Type"])
            .agg(pl.all().sum())
            .fill_null(0)
            .sort(by=["Account ID", "Date", "Currency Split Type", "Security ID"])
        )
        return df

    def adjust_last_date(self, df: pl.DataFrame) -> pl.DataFrame:
        df = df.with_columns(
            pl.when(pl.col("Date") == self.max_date)
            .then(
                (
                    pl.col("Date").str.to_date().cast(pl.Date) - pl.duration(days=1)
                ).dt.strftime("%Y-%m-%d")
            )
            .otherwise(pl.col("Date"))
            .alias("Date")
        )
        return df

    @property
    def portfolios(self):
        portfolios = self.client_data.select(["Account ID"]).unique().sort("Account ID")
        portfolios = portfolios.with_columns(
            (pl.col("Account ID") + "_PrimarySleeve").alias("Firm Provided Key")
        )
        return portfolios

    @property
    def fin_transfers(self):
        fin_transfers = (
            self.client_data.select(
                ["Date", "Account ID", "Security ID", "FinTransfer", "FinTransferIn"]
            )
            .filter((pl.col("FinTransfer") != 0) | (pl.col("FinTransferIn") != 0))
            .rename(
                {
                    "Account ID": "PortfolioID",
                    "Security ID": "InstrumentID",
                    "FinTransfer": "DateFinTransfPosVal",
                    "FinTransferIn": "DateFinTransfInPosVal",
                }
            )
        )
        fin_transfers = fin_transfers.with_columns(
            pl.when(pl.col("InstrumentID") == "USD")
            .then(pl.lit("MAIN"))
            .otherwise(pl.lit("0"))
            .alias("CurrencySplitType")
        )
        return fin_transfers

    @property
    def opr_transfers(self):
        opr_transfers = (
            self.client_data.select(
                ["Date", "Account ID", "Security ID", "OprTransfer"]
            )
            .filter(pl.col("OprTransfer") != 0)
            .rename(
                {
                    "Account ID": "PortfolioID",
                    "Security ID": "InstrumentID",
                    "OprTransfer": "DateOprTransfPosVal",
                }
            )
        )
        opr_transfers = opr_transfers.with_columns(
            pl.when(pl.col("InstrumentID") == "USD")
            .then(pl.lit("MAIN"))
            .otherwise(pl.lit("0"))
            .alias("CurrencySplitType")
        )
        return opr_transfers

    @property
    def book_values(self):
        book_values = self.inputs.select(
            [
                "Date",
                "Portfolio Firm Provided Key",
                "Position Firm Provided Key",
                "Currency Split Type",
                "DateCashFrTrades",
            ]
        ).rename(
            {
                "Portfolio Firm Provided Key": "PortfolioID",
                "Position Firm Provided Key": "InstrumentID",
                "Currency Split Type": "CurrencySplitType",
                "DateCashFrTrades": "DateTradeAmt",
            }
        )
        book_values = (
            pl.concat(
                [book_values, self.fin_transfers, self.opr_transfers], how="diagonal"
            )
            .fill_null(0)
            .group_by(["Date", "PortfolioID", "InstrumentID", "CurrencySplitType"])
            .agg(pl.all().sum())
            .sort(by=["PortfolioID", "Date", "CurrencySplitType", "InstrumentID"])
        )

        book_values = book_values.with_columns(
            pl.lit(0).alias("BookNumUnits"),
            pl.lit(0).alias("BookValue"),
            pl.lit(0).alias("DateTransferredCost"),
            pl.lit(0).alias("DateRealizedPnl"),
            pl.lit(0).alias("InternalBookNumUnits"),
            pl.lit(0).alias("InternalBookValue"),
            pl.lit(0).alias("DateInternalTransferredCost"),
            pl.lit(0).alias("DateInternalRealizedPnl"),
            pl.lit(0).alias("SettledBookValue"),
            pl.lit(0).alias("DateFinTransfAccrVal"),
            pl.lit(0).alias("DateOprTransfAccrVal"),
        )
        book_values = self.adjust_last_date(book_values)
        return book_values

    def create_output_folders(self):
        Path(f"{self.output_path}/inputs").mkdir(parents=True, exist_ok=True)
        Path(f"{self.output_path}/portfolios").mkdir(parents=True, exist_ok=True)
        Path(f"{self.output_path}/bookvalues").mkdir(parents=True, exist_ok=True)

    def write_files(self):
        inputs = self.inputs.join(
            self.household_mapping,
            left_on="Portfolio Firm Provided Key",
            right_on="Account ID",
            how="left",
        )

        portfolios = self.portfolios.join(
            self.household_mapping,
            on="Account ID",
            how="left",
        ).drop("Account ID")

        bookvalues = self.book_values.join(
            self.household_mapping,
            left_on="PortfolioID",
            right_on="Account ID",
            how="left",
        )

        counter = 0

        for household in self.households:
            print(
                f"Processing household {counter+1}/{len(self.households)}: {household}"
            )
            inputs_hh = inputs.filter(pl.col("Household ID") == household).drop(
                "Household ID"
            )
            portfolios_hh = portfolios.filter(pl.col("Household ID") == household).drop(
                ["Household ID"]
            )
            bookvalues_hh = bookvalues.filter(pl.col("Household ID") == household).drop(
                "Household ID"
            )

            inputs_hh.write_csv(
                f"{self.output_path}/inputs/own-analytics-set-{counter}.csv"
            )
            portfolios_hh.write_csv(
                f"{self.output_path}/portfolios/portfolio-set-{counter}.csv",
            )
            Path(f"{self.output_path}/bookvalues/bv-set-{counter}").mkdir(
                parents=True, exist_ok=True
            )
            bookvalues_hh.write_csv(
                f"{self.output_path}/bookvalues/bv-set-{counter}/bv-set-{counter}_USD.csv"
            )

            counter += 1

        print("All files written successfully.")

    def main(self) -> pd.DataFrame:
        self.create_output_folders()
        self.write_files()


if __name__ == "__main__":
    vnf_processor = VnFV52(client="gresham")
    processed_data = vnf_processor.main()
