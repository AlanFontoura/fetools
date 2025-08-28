# src/s3_importer.py

import pandas as pd
import polars as pl

class S3Importer:
    def import_data(self, file_path):
        """
        Import data from an S3 object.
        :param file_path: str, S3 file path in the format s3://bucket/key
        :return: polars DataFrame
        """
        try:
            dataframe = pd.read_csv(file_path)
        except FileNotFoundError as e:
            raise FileNotFoundError(f"File not found: {file_path}")
        return pl.from_pandas(dataframe)