import pandas as pd


class GetEoMData:

    def __init__(self, file_path, date_column="Date", chunk_size=100_000):
        self.file_path = file_path
        self.date_column = date_column
        self._historical_data = None
        self._eom_dates = None

    @property
    def eom_dates(self) -> list:
        if self._eom_dates is not None:
            return self._eom_dates
        self._eom_dates = (
            pd.date_range(start="1970-01-31", end="2099-12-31", freq="ME")
            .strftime("%Y-%m-%d")
            .tolist()
        )
        return self._eom_dates

    def get_eom_data(self) -> pd.DataFrame:
        file_name = self.file_path.split("/")[-1]
        counter = 1
        chunks = []
        chunk_size = 100_000

        for chunk in pd.read_csv(self.file_path, chunksize=chunk_size):
            if chunk.shape[0] == chunk_size:
                print(
                    f"Processed {counter*chunk_size:,} rows of file {self.file_name}..."
                )
            else:
                print(
                    f"Processed total of {((counter-1)*chunk_size)+chunk.shape[0]:,} rows of file {self.file_path}."
                )
            counter += 1
            chunk = chunk[chunk[self.date_column].isin(self.eom_dates)]
            chunks.append(chunk)

        return pd.concat(chunks, ignore_index=True)
