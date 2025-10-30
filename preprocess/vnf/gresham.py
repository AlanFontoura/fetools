import pandas as pd
import polars as pl
import awswrangler as wr
from fetools.importer import DataImporter


class PreProcessVnFData:
    """
    Preprocess Gresham VnF data from a CSV file.

    Args:
        file_path (str): The path to the CSV file containing Gresham VnF data.
    """

    def __init__(self, file_path: str):
        self.data_importer = DataImporter()
        self.file_path = file_path
        self.usd_instrument = "BC36B21F-D673-492F-8D1D-30956550D237"
        self.custodian_data = "s3://d1g1t-custodian-data-us-east-1/apx/gresham"

    def get_vnf_data(self) -> pl.DataFrame:
        if self.file_path.startswith("s3://"):
            data = wr.s3.read_csv(self.file_path)
            return pl.from_pandas(data)

        data = self.data_importer.import_data(source="csv", file_path=self.file_path)
        return pl.from_pandas(data)

    @property
    def last_business_day(self) -> str:
        last_business_day = pd.Timestamp.today() - pd.offsets.BDay(1)
        return last_business_day.strftime("%Y%m%d")

    def security_type_mapping(self) -> pl.DataFrame:
        security_data = self.get_security_data()
        pe_likes = ["en", "hf", "hp", "lp", "oa", "pl", "pp", "zp", "pe", "rp"]
        security_data = security_data.with_columns(
            (
                pl.when(pl.col("SecurityTypeCode").str.to_lowercase().is_in(pe_likes))
                .then(pl.lit("Non Marketable"))
                .when(pl.col("SecurityTypeCode").str.to_lowercase() == "ca")
                .then(pl.lit("Cashlike"))
                .otherwise(pl.lit("Marketable"))
            ).alias("SecurityType")
        ).drop("SecurityTypeCode")
        return security_data

    def get_security_data(self) -> pl.DataFrame:
        file_path = f"{self.custodian_data}/{self.last_business_day}/Security.csv"
        security_data = self.data_importer.import_data(source="s3", file_path=file_path)
        security_data = security_data.select(["SecurityID", "SecurityTypeCode"])
        return security_data

    def filter_data(self, df: pl.DataFrame) -> pl.DataFrame:
        cols = [
            "Date",
            "AccountCode",
            "SecurityID",
            "Value",
            "NumUnits",
            "Total_By",
            "Total_Sl",
            "Total_Li",
            "Total_Lo",
            "Total_Ti",
            "Total_To",
            "Dp- epus",
            "Dp- exus",
            "Wd- epus",
            "Wd- exus",
        ]
        filtered_df = df.select(cols)
        return filtered_df

    def adjust_units(self, df: pl.DataFrame) -> pl.DataFrame:
        security_data = self.security_type_mapping()
        if "SecurityID" not in df.columns:
            df = df.with_columns(pl.lit("legacy_instrument_USD").alias("SecurityID"))
        df = df.join(security_data, on="SecurityID", how="left")
        adjusted_df = df.with_columns(
            pl.when(pl.col("SecurityType") == "Cashlike")
            .then(pl.col("Value"))
            .when(pl.col("SecurityID") == self.usd_instrument)
            .then(0)
            .when(pl.col("SecurityType") == "Non Marketable")
            .then(pl.col("Value") / 100_000)
            .otherwise(pl.col("NumUnits"))
            .alias("NumUnits")
        )
        return adjusted_df

    def transform_data(self, df: pl.DataFrame) -> pl.DataFrame:
        transformed_df = df.with_columns(
            pl.col("SecurityID").str.replace_all(rf"^{self.usd_instrument}$", "USD"),
            (pl.col("Total_By") - pl.col("Total_Sl")).alias("DateCashFrTrades"),
            (pl.col("Total_Li") - pl.col("Total_Lo")).alias("FinTransfer"),
            (pl.col("Total_Li")).alias("FinTransferIn"),
            (pl.col("Total_Ti") - pl.col("Total_To")).alias("OprTransfer"),
            (pl.col("Dp- epus") - pl.col("Wd- epus")).alias("DateFees"),
            (pl.col("Dp- exus") - pl.col("Wd- exus")).alias("DateExpenses"),
            (pl.col("AccountCode").str.split("-").list.head(2).list.join("-")).alias(
                "Household ID"
            ),
        ).drop(
            [
                "Total_By",
                "Total_Sl",
                "Total_Li",
                "Total_Lo",
                "Total_Ti",
                "Total_To",
                "Dp- epus",
                "Dp- exus",
                "Wd- epus",
                "Wd- exus",
            ]
        )
        transformed_df = transformed_df.rename(
            {
                "AccountCode": "Account ID",
                "SecurityID": "Security ID",
            }
        )
        return transformed_df.sort(
            by=["Household ID", "Account ID", "Date", "Security ID"]
        )

    def remove_empty_rows(self, df: pl.DataFrame) -> pl.DataFrame:
        numeric_cols = df.select(pl.col(pl.NUMERIC_DTYPES)).columns
        clean_df = df.filter(~pl.all_horizontal(pl.col(numeric_cols) == 0))
        return clean_df

    def pre_process(self) -> pl.DataFrame:
        data = self.get_vnf_data()
        data = self.adjust_units(data)
        data = self.filter_data(data)
        data = self.transform_data(data)
        data = self.remove_empty_rows(data)
        return data


if __name__ == "__main__":
    k = PreProcessVnFData(
        file_path="s3://d1g1t-production-file-transfer-us-east-1/gresham/for_d1g1t/PnL/2025-10-10/SecurityLevel/"
    )
    k.pre_process().write_csv("test.csv")
