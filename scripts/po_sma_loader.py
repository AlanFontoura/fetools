from fetools.importer import DataImporter
from constants.constants import FILE_LOCATIONS
import polars as pl
import pandas as pd
import awswrangler as wr


class PartialOwnershipSMA:
    def __init__(self, date: str, client: str):
        self.date = date.replace("-", "")
        self.client = client.lower()
        self._accounts = None
        self._ownerships = None

    def __repr__(self):
        return f"PartialOwnershipSMA(account_id={self.account_id}, account_name={self.account_name})"

    @property
    def files(self):
        return FILE_LOCATIONS.get(self.client, {})

    @property
    def data_importer(self):
        return DataImporter()

    @property
    def custodian_bucket(self):
        custodian_bucket = self.files.get("custodian_bucket")
        if not custodian_bucket:
            raise ValueError(f"No custodian bucket found for client: {self.client}")
        return custodian_bucket

    @property
    def accounts(self):
        if self._accounts is not None:
            return self._accounts
        accounts_data = self.data_importer.import_data(
            source="s3", file_path=f"{self.custodian_bucket}{self.date}/Account.csv"
        )
        clients_data = self.data_importer.import_data(
            source="s3", file_path=f"{self.custodian_bucket}{self.date}/Client.csv"
        )
        accounts_data = accounts_data.join(
            clients_data, left_on="ClientCode", right_on="ClientID", how="left"
        )
        accounts_data = accounts_data.select(
            [
                pl.col("AccountCode").alias("account_id"),
                pl.col("AccountName").alias("account_name"),
                pl.col("ClientCode").alias("client_id"),
                pl.col("ClientName").alias("client_name"),
                (pl.col("IsSMA") == 1).alias("sma"),
                pl.col("SMAName").alias("sma_name"),
                pl.col("AssetCategory").alias("asset_category"),
                pl.col("AssetClass").alias("asset_class"),
                pl.col("SubAssetClass").alias("asset_subclass"),
                pl.col("AssetClassLevel3").alias("asset_class_level3"),
            ]
        ).sort(by=["client_name", "client_id", "account_name", "account_id"])
        self._accounts = accounts_data
        return self._accounts

    @property
    def ownerships(self):
        if self._ownerships is not None:
            return self._ownerships
        ownerships = self.data_importer.import_data(
            source="s3", file_path=f"{self.custodian_bucket}{self.date}/LEOwnership.csv"
        )
        ownerships = ownerships.select(
            [
                pl.col("Client ID").alias("owned_client_id"),
                pl.col("Parent ID").alias("owner_client_id"),
                pl.col("Date").alias("date"),
                (pl.col("Percent").cast(pl.Float32) / 100)
                .round(6)
                .alias("ownership_percentage"),
            ]
        ).sort(by=["owned_client_id", "owner_client_id", "date"])
        self._ownerships = ownerships
        return self._ownerships

    def validate_ownerships(self):
        has_errors = False

        invalid_owners = self.ownerships.filter(
            ~self.ownerships["owner_client_id"].is_in(self.accounts["client_id"])
        )
        if invalid_owners.height > 0:
            print(f"Invalid Parent ID found: {invalid_owners}")
            invalid_owners.write_csv("outputs/invalid_parent_id.csv")
            has_errors = True

        invalid_owned = self.ownerships.filter(
            ~self.ownerships["owned_client_id"].is_in(self.accounts["client_id"])
        )
        if invalid_owned.height > 0:
            print(f"Invalid Client ID found: {invalid_owned}")
            invalid_owned.write_csv("outputs/invalid_client_id.csv")
            has_errors = True

        negative_ownerships = self.ownerships.filter(
            self.ownerships["ownership_percentage"] < 0
        )
        if negative_ownerships.height > 0:
            print(f"Negative ownership percentages found: {negative_ownerships}")
            negative_ownerships.write_csv("outputs/negative_ownerships.csv")
            has_errors = True

        total_ownerships = self.ownerships.group_by(["owned_client_id", "date"]).agg(
            pl.col("ownership_percentage").sum().round(4).alias("total_percentage")
        )
        invalid_totals = total_ownerships.filter(
            total_ownerships["total_percentage"] != 1.0
        )
        invalid_entries = self.ownerships.join(
            invalid_totals, on=["owned_client_id", "date"], how="inner"
        ).sort(by=["owned_client_id", "date", "owner_client_id"])
        if invalid_entries.height > 0:
            print(f"Ownership percentages not summing to 100% found: {invalid_entries}")
            invalid_entries.write_csv("outputs/invalid_total_ownerships.csv")
            has_errors = True

        duplicate_entries = (
            self.ownerships.group_by(["owned_client_id", "owner_client_id", "date"])
            .agg(pl.count().alias("count"))
            .filter(pl.col("count") > 1)
        )
        if duplicate_entries.height > 0:
            print(f"Duplicate ownership entries found: {duplicate_entries}")
            duplicate_entries.write_csv("outputs/duplicate_ownerships.csv")
            has_errors = True

        if has_errors:
            raise ValueError(
                "Ownership validation failed. Check output CSV files for details."
            )

    def run(self):
        self.validate_ownerships()


a = PartialOwnershipSMA(date="20250908", client="gresham")
a.run()
