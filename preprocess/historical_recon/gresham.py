import pandas as pd
import awswrangler as wr
from fetools.get_eom_data import GetEoMData
from constants.constants import FILE_LOCATIONS


class GetTrackingData:
    def __init__(
        self, period="monthly", env="production", output_file_name="tracking_data"
    ):
        self.file_path = f'{FILE_LOCATIONS["gresham"]["exports_folder"]}{env}-gresham-{period}-tracking.csv'
        self.output_file_name = output_file_name

    def tracking_data(self) -> pd.DataFrame:
        data_filterer = GetEoMData(
            file_path=self.file_path, date_column="date", chunk_size=100_000
        )
        tracking_data = data_filterer.get_eom_data()
        return tracking_data

    def format_tracking_data(self, tracking_data: pd.DataFrame) -> pd.DataFrame:
        tracking_data = tracking_data.rename(
            columns={
                "date": "Date",
                "account": "Account ID",
                "instrument": "Security ID",
                "units": "Units",
                "price": "Price",
                "mv": "Market Value",
            }
        )
        tracking_data = tracking_data[tracking_data["is_dead"] != "t"]
        tracking_data.loc[tracking_data["Security ID"] == "USD", "Market Value"] = (
            tracking_data.loc[tracking_data["Security ID"] == "USD", "Units"]
        )
        cols = ["Date", "Account ID", "Security ID", "Units", "Price", "Market Value"]
        tracking_data = tracking_data[cols]
        tracking_data = (
            tracking_data.groupby(["Date", "Account ID", "Security ID"])
            .sum()
            .reset_index(drop=False)
        )
        tracking_data = tracking_data.sort_values(
            by=["Account ID", "Security ID", "Date"]
        )
        return tracking_data

    def main(self) -> None:
        tracking_data = self.tracking_data()
        formatted_tracking_data = self.format_tracking_data(tracking_data)
        formatted_tracking_data.to_pickle(
            f"outputs/historical_position_files/gresham/{self.output_file_name}.pkl"
        )
        print("Gresham tracking data processed and saved.")


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
        file_filterer = GetEoMData(
            file_path=file_path, date_column="Date", chunk_size=chunk_size
        )
        positions = file_filterer.get_eom_data()
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

        return positions[cols]

    def get_all_positions(self, chunk_size=100_000) -> pd.DataFrame:
        all_positions = []
        for file in self.position_files:
            positions = self.get_file(file, chunk_size=chunk_size)
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
