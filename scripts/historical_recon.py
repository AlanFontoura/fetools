import pandas as pd
import importlib


class HistoricalRecon:
    def __init__(
        self,
        client_name: str,
        start_date: str,
        end_date: str,
        tracking_data: pd.DataFrame,
        client_data: pd.DataFrame,
    ):
        self.client_name = client_name
        self.start_date = start_date
        self.end_date = end_date
        self.tracking_data = tracking_data
        self.client_data = client_data

    def filter_data_by_date(self, data: pd.DataFrame) -> pd.DataFrame:
        return data[(data["Date"] >= self.start_date) & (data["Date"] <= self.end_date)]

    def run(self) -> None:
        module = importlib.import_module(self.module_name)
        class_name = "".join(word.title() for word in self.module_name.split("_"))
        recon_class = getattr(module, class_name)(*self.args, **self.kwargs)
        recon_class.main()
