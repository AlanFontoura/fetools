import pandas as pd
from dataclasses import dataclass, asdict
import json


@dataclass(frozen=True)
class ColumnDefinitions:
    SOURCE: str
    COL_NAME: str | None = None
    PREFIX: str | None = None
    SUFFIX: str | None = None
    VALUE: str | float | None = None


@dataclass(frozen=True)
class FileDefinitions:
    FILENAME: str
    OUTPUT_PATH: str
    COLUMNS: list[ColumnDefinitions]


class DataFrameChanger:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def modify_column(
        self,
        column_def: ColumnDefinitions,
    ) -> pd.DataFrame:
        col_name = column_def.COL_NAME or column_def.SOURCE
        if column_def.PREFIX:
            self.df[col_name] = [
                f"{column_def.PREFIX}{entry}"
                for entry in self.df[column_def.SOURCE]
            ]
        if column_def.SUFFIX:
            self.df[col_name] = [
                f"{entry}{column_def.SUFFIX}"
                for entry in self.df[column_def.SOURCE]
            ]
        if column_def.VALUE is not None:
            self.df[col_name] = column_def.VALUE
        return self.df

    def modify_columns(
        self,
        column_defs: list[ColumnDefinitions],
        extra_columns: list[str] = [],
    ) -> pd.DataFrame:
        for column_def in column_defs:
            self.df = self.modify_column(column_def)
        keep_columns = [
            column_def.COL_NAME if column_def.COL_NAME else column_def.SOURCE
            for column_def in column_defs
        ] + extra_columns
        return self.df[keep_columns]


class FileGenerator:
    def __init__(self, data: pd.DataFrame, file_def: FileDefinitions):
        self.data = DataFrameChanger(data)
        self.file_def = file_def

    def generate_file(self) -> pd.DataFrame:
        self.data.df = self.data.modify_columns(self.file_def.COLUMNS)
        return self.data.df


def load_file_definitions(path: str) -> dict[str, FileDefinitions]:
    with open(path, "r") as f:
        raw_defs = json.load(f)
    file_defs = {}
    for key, value in raw_defs.items():
        column_defs = [
            ColumnDefinitions(**col_def) for col_def in value["COLUMNS"]
        ]
        file_defs[key] = FileDefinitions(
            FILENAME=value["FILENAME"],
            OUTPUT_PATH=value["OUTPUT_PATH"],
            COLUMNS=column_defs,
        )
    return file_defs
