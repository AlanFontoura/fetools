import pandas as pd
import polars as pl
import importlib
from pathlib import Path


class PartialOwnershipRecon:
    def __init__(self, client, period="monthly"):
        self.period = period
        self.client = client
        self._tracking_data = None

    @property
    def tracking_data(self) -> pl.DataFrame:
        if self._tracking_data is not None:
            return self._tracking_data
        try:
            preprocessor = importlib.import_module(
                f"preprocess.po_recon.{self.client.lower()}"
            )
        except ModuleNotFoundError:
            raise ValueError(
                f"Preprocessor for client '{self.client}' not found. Check the 'preprocess/po_recon' folder."
            )
            sys.exit(1)

        client_preprocessor = preprocessor.PreProcessPartialOwnershipData(
            period=self.period
        )
        self._tracking_data = client_preprocessor.get_data()
        return self._tracking_data

    def group_tracking(self, account_type: str) -> pl.DataFrame:
        grouped_data = (
            self.tracking_data.filter(pl.col("account_type") == account_type)
            .select(["original_account", "date", "mv"])
            .group_by(["original_account", "date"])
            .agg(pl.col("mv").sum().round(2).alias(f"{account_type}_mv"))
        ).sort(["original_account", "date"])
        return grouped_data

    def compare_market_values(self) -> pl.DataFrame:
        direct_mv = self.group_tracking("direct")
        split_mv = self.group_tracking("split")
        original_mv = self.group_tracking("original")
        comparison = (
            original_mv.join(direct_mv, on=["original_account", "date"], how="full")
            .drop(["original_account_right", "date_right"])
            .join(split_mv, on=["original_account", "date"], how="full")
            .drop(["original_account_right", "date_right"])
        )
        return comparison.fill_null(0).sort(["original_account", "date"])

    def add_recon_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        df = df.with_columns(
            [
                (
                    abs(pl.col("original_mv").round(2) - pl.col("direct_mv").round(2))
                    <= 0.02
                ).alias("direct_recon"),
                (
                    abs(pl.col("original_mv").round(2) - pl.col("split_mv").round(2))
                    <= 0.02
                ).alias("split_recon"),
            ]
        )
        return df

    def main(self) -> None:
        comparison = self.compare_market_values()
        comparison_with_recon = self.add_recon_columns(comparison)
        return comparison_with_recon


if __name__ == "__main__":
    client_name = "gresham"  # Example client name
    period = "monthly"  # Example period

    po_recon = PartialOwnershipRecon(client=client_name, period=period)
    po_recon.main().write_csv("outputs/po_recon/gresham/po_recon.csv")
