# src/local_importer.py

import pandas as pd
import polars as pl


class LocalImporter:
    def import_data(self, file_path, file_type="csv"):
        """
        Import data from a local file.
        :param file_path: str, path to the file
        :param file_type: str, "csv" or "pickle"
        :return: polars DataFrame
        """
        if file_type == "pkl":
            data = pd.read_pickle(file_path)
            return pl.from_pandas(data)
        elif file_type == "csv":
            return pl.read_csv(file_path)
        else:
            raise ValueError(f"Unsupported file type: {file_type}")
