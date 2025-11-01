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
        chunk_size = 100_000
        try:
            counter = 1
            for chunk in pd.read_csv(file_path, chunksize=chunk_size):
                dataframe = (
                    chunk
                    if "dataframe" not in locals()
                    else pd.concat([dataframe, chunk])
                )
                if chunk.shape[0] < chunk_size:
                    print(f"Final chunk processed with {dataframe.shape[0]:,} rows.")
                else:
                    print(f"Processed {counter*chunk_size:,} rows...")
                counter += 1
            print(f"Total rows processed: {len(dataframe)}")
        except FileNotFoundError as e:
            raise FileNotFoundError(f"File not found: {file_path}")
        return pl.from_pandas(dataframe)
