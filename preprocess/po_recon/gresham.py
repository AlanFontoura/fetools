import pandas as pd
import polars as pl
import awswrangler as wr
from tools.importer import DataImporter


class PreProcessPartialOwnershipData:

    def __init__(self, period="monthly"):
        self.data_importer = DataImporter()
        self.file_path = f"s3://d1g1t-client-us/gresham/exports/production-gresham-{period}-tracking.csv"
        self._tracking_data = None
        self._original_account_list = None

    @property
    def tracking_data(self) -> pl.DataFrame:
        if self._tracking_data is not None:
            return self._tracking_data
        self._tracking_data = self.data_importer.import_data(
            source="s3", file_path=self.file_path
        )
        return self._tracking_data

    @property
    def original_account_list(self) -> list:
        if self._original_account_list is not None:
            return self._original_account_list
        original_account_list = [
            account.replace("_client", "")
            for account in self.tracking_data["account"].to_list()
            if account.endswith("_client")
        ]
        self._original_account_list = list(set(original_account_list))
        return self._original_account_list

    def categorize_accounts(self) -> pl.DataFrame:
        tracking = self.tracking_data.with_columns(
            pl.col("account").str.split("_").list.get(0).alias("original_account")
        )
        tracking = tracking.filter(
            pl.col("original_account").is_in(self.original_account_list)
        )
        tracking = tracking.with_columns(
            pl.when(pl.col("account") == pl.col("original_account"))
            .then(pl.lit("original"))
            .when(pl.col("account").str.ends_with("_client"))
            .then(pl.lit("direct"))
            .otherwise(pl.lit("split"))
            .alias("account_type")
        )
        return tracking

    def fill_usd_market_value(self, tracking: pl.DataFrame) -> pl.DataFrame:
        return tracking.with_columns(
            pl.when(pl.col("instrument") == "USD")
            .then(pl.col("units"))
            .otherwise(pl.col("mv"))
            .alias("mv")
        )

    def get_data(self) -> pl.DataFrame:
        data = self.categorize_accounts()
        data = self.fill_usd_market_value(data)
        return data.select(
            [
                "account_type",
                "original_account",
                "account",
                "date",
                "instrument",
                "units",
                "mv",
            ]
        ).sort(["original_account", "account_type", "account", "date", "instrument"])


if __name__ == "__main__":
    preprocessor = PreProcessPartialOwnershipData(period="monthly")
    print(preprocessor.get_data())
