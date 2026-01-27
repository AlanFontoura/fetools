import pandas as pd
import json
from pathlib import Path


class FileGenerator:
    def __init__(
        self,
        data: pd.DataFrame,
        file_defs: dict[str, dict],
        save: bool = True,
    ):
        self.data = data
        self.file_defs = file_defs
        self.save = save

    def generate_file(self, file_def: dict) -> pd.DataFrame:
        df_changer = DataFrameChanger(self.data)
        df_changer.change_columns(file_def)
        columns = list(file_def.keys())
        return df_changer.df[columns]

    def create_folders(self, filename: str, file_def: dict) -> Path:
        output_folder = file_def.get("folder", filename)
        output_path = Path("data/outputs", output_folder)
        output_path.mkdir(parents=True, exist_ok=True)
        return output_path

    def generate_files(self) -> None:
        for filename, configs in self.file_defs.items():
            file = self.generate_file(configs["cols"])
            output_path = self.create_folders(filename, configs)
            if self.save:
                file.to_csv(Path(output_path, f"{filename}.csv"), index=False)


class DataFrameChanger:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def change_columns(
        self,
        configs: dict[str, dict],
    ) -> None:
        for target_col, config in configs.items():
            if "value" in config:
                self.add_constant_column(target_col, config["value"])
            else:
                self.copy_column(
                    source_col=config["source"],
                    target_col=target_col,
                    prefix=config.get("prefix", ""),
                    suffix=config.get("suffix", ""),
                )

    def copy_column(
        self,
        source_col: str,
        target_col: str,
        prefix: str = "",
        suffix: str = "",
    ) -> None:
        self.df[target_col] = [
            f"{prefix}{entry}{suffix}" for entry in self.df[source_col]
        ]

    def add_constant_column(
        self, target_col: str, value: str | float
    ) -> None:
        self.df[target_col] = value


def main():
    data = pd.read_csv("data/inputs/SMA_example.csv")
    with open("src/configs/sma_mapping.json", "r") as f:
        file_defs = json.load(f)
    SMA = FileGenerator(data, file_defs)
    SMA.generate_files()


if __name__ == "__main__":
    main()
