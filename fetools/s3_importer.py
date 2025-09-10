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
            counter = 1
            for chunk in pd.read_csv(file_path, chunksize=100_000):
                dataframe = (
                    chunk
                    if "dataframe" not in locals()
                    else pd.concat([dataframe, chunk])
                )
                print(f"Processed {counter*100_000} rows...")
                counter += 1
            print(f"Total rows processed: {len(dataframe)}")
        except FileNotFoundError as e:
            raise FileNotFoundError(f"File not found: {file_path}")
        return pl.from_pandas(dataframe)
