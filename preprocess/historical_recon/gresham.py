import pandas as pd
import awswrangler as wr
from constants.constants import FILE_LOCATIONS


class PreProcessHistoricalData:
    def __init__(self, date):
        self.data_importer = DataImporter()
        self.file_path = f'{FILE_LOCATIONS["gresham"]["position_files"]}{date}/'

    @property
    def historical_data(self) -> pd.DataFrame:
        if self._historical_data is not None:
            return self._historical_data
        self._historical_data = self.data_importer.import_data(
            source="s3", file_path=self.file_path
        )
        return self._historical_data


class GetPositionFiles:
    def __init__(self, date, output_file_name="client_positions"):
        self.file_path = f'{FILE_LOCATIONS["gresham"]["position_files"]}{date}/'
        self.output_file_name = output_file_name

    @property
    def position_files(self):
        files = wr.s3.list_objects(self.file_path)
        files = [file for file in files if "Position" in file]
        return files

    def get_file(self, file_path, chunk_size=100_000) -> pd.DataFrame:
        eom_dates = (
            pd.date_range(start="1970-01-31", end="2099-12-31", freq="ME")
            .strftime("%Y-%m-%d")
            .tolist()
        )

        file_name = file_path.split("/")[-1]

        cols = [
            "Date",
            "AccountCode",
            "SecurityID",
            "Symbol",
            "Type",
            "Current",
            "Price",
            "MV_Local",
        ]

        chunks = []
        counter = 1
        for chunk in pd.read_csv(file_path, chunksize=chunk_size):
            if chunk.shape[0] == chunk_size:
                print(f"Processed {counter*chunk_size:,} rows of file {file_name}...")
            else:
                print(
                    f"Processed total of {((counter-1)*chunk_size)+chunk.shape[0]:,} rows of file {file_name}."
                )
            counter += 1
            chunk = chunk[chunk["Date"].isin(eom_dates)]
            chunk = chunk[cols]
            chunks.append(chunk)

        positions = pd.concat(chunks, ignore_index=True)
        return positions

    def get_all_positions(self) -> pd.DataFrame:
        all_positions = []
        for file in self.position_files:
            positions = self.get_file(file)
            all_positions.append(positions)
        all_positions_df = pd.concat(all_positions, ignore_index=True)
        return all_positions_df

    def format_positions(self, positions: pd.DataFrame) -> pd.DataFrame:
        positions = positions.rename(
            columns={
                "AccountCode": "Account ID",
                "SecurityID": "Security ID",
                "Current": "Units",
                "MV_Local": "Market Value",
            }
        )
        positions = (
            positions.groupby(["Date", "Account ID", "Security ID", "Symbol", "Type"])
            .sum()
            .reset_index(drop=False)
            .sort_values(by=["Account ID", "Security ID", "Date"])
        )
        return positions

    def main(self) -> None:
        positions = self.get_all_positions()
        formatted_positions = self.format_positions(positions)
        formatted_positions.to_pickle(
            f"outputs/historical_position_files/gresham/{self.output_file_name}.pkl"
        )
        print("Gresham position files processed and saved.")


if __name__ == "__main__":
    date = "2025-10-24"
    processor = GetPositionFiles(date)
    processor.main()
