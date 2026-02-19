import pandas as pd
import json
from typing import Any
from dataclasses import dataclass
from multiprocess import Pool
from tqdm import tqdm
from fetools.api.base_main import ReportGeneric
from fetools.utils.exceptions import NoResponseError


@dataclass
class TrendAUMPair:
    account_fpk: str
    account_entity_id: str
    class_series_fpk: str
    class_series_entity_id: str
    currency: str


class DownloadTrendAUM(ReportGeneric):
    def __init__(self):
        super().__init__()
        self._trend_aum_payload = None

    def add_extra_args(self):
        self.parser.add_argument(
            "-d",
            "--date",
            dest="date",
            type=str,
            required=True,
            help="End date to be used",
        )

    @property
    def trend_aum_payload(self):
        if self._trend_aum_payload is None:
            with open("data/templates/payloads/trend_aum.json", "r") as f:
                self._trend_aum_payload = json.load(f)
                self._trend_aum_payload["settings"]["date"][
                    "date"
                ] = self.args.date
        return self._trend_aum_payload

    @staticmethod
    def clean_url(url: str) -> str:
        return url.split("/")[-2] if url else url

    def clean_column(self, col: pd.Series) -> list[str]:
        return [self.clean_url(val) for val in col]

    def get_accounts(self):
        accounts = self.get_large_data(
            "accounts",
            fields=[
                "firm_provided_key",
                "entity_id",
                "class_series",
                "currency",
            ],
        )
        accounts["currency"] = self.clean_column(accounts["currency"])
        accounts["class_series"] = self.clean_column(accounts["class_series"])
        return accounts

    def get_class_series(self):
        fund_call = self.api.funds
        fund_call._store["base_url"] += "class-series/"
        response = fund_call.get(
            extra="limit=10000&fields=firm_provided_key,entity_id"
        )
        if not response:
            raise NoResponseError("Request returned no result!")
        class_series_df = pd.DataFrame(response["results"])
        class_series_df = class_series_df[
            ["firm_provided_key", "entity_id"]
        ].rename(
            columns={
                "firm_provided_key": "class_series",
                "entity_id": "class_series_entity_id",
            }
        )
        return class_series_df

    def filter_accounts(self, df: pd.DataFrame) -> pd.DataFrame:
        special_accounts = (
            df[
                (df["firm_provided_key"].str.startswith("sma_account_"))
                | (df["firm_provided_key"].str.startswith("po_direct_"))
            ]
            .copy()
            .drop(columns="class_series")
            .rename(columns={"firm_provided_key": "account_id"})
        )
        special_accounts["original_account"] = [
            acc.replace("sma_account_", "").replace("po_direct_", "")
            for acc in special_accounts["account_id"]
        ]
        special_accounts = special_accounts.merge(
            df[["firm_provided_key", "class_series"]],
            left_on="original_account",
            right_on="firm_provided_key",
            how="left",
        ).drop(columns="original_account")

        class_series = self.get_class_series()
        special_accounts = special_accounts.merge(
            class_series,
            how="left",
        )
        return special_accounts

    def modify_trend_aum_payload(
        self, entity_type: str, entity_id: str, currency: str
    ) -> Any:
        payload = self.trend_aum_payload
        if entity_type == "account":
            payload["control"]["selected_entities"] = {
                "accounts_or_positions": [[entity_id]]
            }
        elif entity_type == "class_series":
            payload["control"]["selected_entities"] = {
                "class_series": [entity_id]
            }
        else:
            return {}
        payload["settings"]["currency"] = currency
        return payload

    def get_trend_aum(
        self, entity_type: str, entity_id: str, currency: str, fpk: str
    ) -> pd.DataFrame:
        payload = self.modify_trend_aum_payload(
            entity_type, entity_id, currency
        )
        try:
            trend_aum_df = self.get_calculation("trend-aum", payload)
        except:
            return pd.DataFrame()
        if entity_type == "account":
            trend_aum_df["Account ID"] = fpk
            trend_aum_df = trend_aum_df.rename(
                columns={"Total Portfolio": "Account MV"}
            )
        elif entity_type == "class_series":
            trend_aum_df["Class Series"] = fpk
            trend_aum_df = trend_aum_df.rename(
                columns={"Total Portfolio": "Class Series MV"}
            )

        trend_aum_df["Date"] = trend_aum_df["Date"].dt.strftime("%Y-%m-%d")
        return trend_aum_df

    def get_trend_aum_pair(self, pair_info: TrendAUMPair) -> pd.DataFrame:
        account_trend_aum = self.get_trend_aum(
            "account",
            pair_info.account_entity_id,
            pair_info.currency,
            pair_info.account_fpk,
        )
        class_series_trend_aum = self.get_trend_aum(
            "class_series",
            pair_info.class_series_entity_id,
            pair_info.currency,
            pair_info.class_series_fpk,
        )

        if account_trend_aum.empty and class_series_trend_aum.empty:
            return pd.DataFrame()

        elif not (account_trend_aum.empty or class_series_trend_aum.empty):
            merged_df = account_trend_aum.merge(
                class_series_trend_aum,
                how="outer",
            )
            return merged_df

        elif not account_trend_aum.empty:
            account_trend_aum["Class Series"] = pair_info.class_series_fpk
            account_trend_aum["Class Series MV"] = None
            return account_trend_aum

        else:
            class_series_trend_aum["Account ID"] = pair_info.account_fpk
            class_series_trend_aum["Account MV"] = None
            return class_series_trend_aum

    def convert_df_to_configs(self, df: pd.DataFrame) -> list[TrendAUMPair]:
        pairs = []
        for _, row in df.iterrows():
            pair = TrendAUMPair(
                account_fpk=row["account_id"],
                account_entity_id=row["entity_id"],
                class_series_fpk=row["class_series"],
                class_series_entity_id=row["class_series_entity_id"],
                currency=row["currency"],
            )
            pairs.append(pair)
        return pairs

    def get_all_trend_aum(
        self, pairs: list[TrendAUMPair]
    ) -> pd.DataFrame | Any:
        with Pool() as pool:
            all_trend_aum = list(
                tqdm(
                    pool.imap(self.get_trend_aum_pair, pairs),
                    total=len(pairs),
                )
            )

        return pd.concat(all_trend_aum, ignore_index=True)

    def after_login(self):
        accounts = self.get_accounts()
        accounts.to_csv("accounts.csv")
        # accounts = pd.read_csv("accounts.csv")
        filtered_accounts = self.filter_accounts(accounts)
        pairs = self.convert_df_to_configs(filtered_accounts)
        all_trend_aum_df = self.get_all_trend_aum(pairs)
        all_trend_aum_df.to_csv("trend_aum.csv", index=False)


if __name__ == "__main__":
    DownloadTrendAUM().main()
