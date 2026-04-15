import pandas as pd
import json
from typing import Any
from dataclasses import dataclass
from multiprocess import Pool
from tqdm import tqdm
from fetools.api.base_main import ReportGeneric
from fetools.utils.exceptions import NoResponseError
from math import ceil


class DownloadCPH(ReportGeneric):
    def __init__(self):
        super().__init__()
        self._cph_payload = None

    # def add_extra_args(self):
    #     self.parser.add_argument(
    #         "-start",
    #         "--start_date",
    #         dest="start_date",
    #         type=str,
    #         required=True,
    #         help="Start date to be used",
    #     )

    #     self.parser.add_argument(
    #         "-end",
    #         "--end_date",
    #         dest="end_date",
    #         type=str,
    #         required=True,
    #         help="End date to be used",
    #     )

    #     self.parser.add_argument(
    #         "-c",
    #         "--currency",
    #         dest="currency",
    #         type=str,
    #         required=True,
    #         help="Currency to be used",
    #     )

    #     self.parser.add_argument(
    #         "-lv",
    #         "--level",
    #         dest="level",
    #         type=str,
    #         required=True,
    #         help="Level to be used",
    #     )

    #     self.parser.add_argument(
    #         "-e",
    #         "--entity_id",
    #         dest="entity_id",
    #         type=str,
    #         required=True,
    #         help="Entity ID to be used",
    #     )

    @property
    def cph_payload(self):
        if self._cph_payload is None:
            with open("data/templates/payloads/cph_table.json", "r") as f:
                payload = json.load(f)
            payload["settings"]["currency"] = self.args.currency
            payload["groups"]["selected"][0][
                "grouping_criterion"
            ] = f"https://{self.args.server}/api/v1/constants/groupings/security-id/"
            if self.args.level == "account":
                payload["control"]["selected_entities"] = {
                    "accounts_or_positions": [[self.args.entity_id]]
                }
            elif self.args.level == "client":
                payload["control"]["selected_entities"] = {
                    "clients": [self.args.entity_id]
                }
            elif self.args.level == "household":
                payload["control"]["selected_entities"] = {
                    "households": [self.args.entity_id]
                }
            else:
                raise ValueError(
                    "Level must be one of 'account', 'client', or 'household'"
                )
            self._cph_payload = payload

        return self._cph_payload

    def get_cph(self, date: str) -> pd.DataFrame:
        payload = self.cph_payload
        payload["settings"]["date"]["date"] = date
        try:
            response = self.get_calculation(
                "cph-table",
                payload=payload,
                v2=True,
            )
            response["Date"] = date
        except NoResponseError:
            return pd.DataFrame()
        return response

    def get_all_cph(self) -> pd.DataFrame:
        dates = pd.date_range(
            start=self.args.start_date, end=self.args.end_date, freq="D"
        ).strftime("%Y-%m-%d")
        with Pool() as pool:
            cph_dfs = list(
                tqdm(pool.imap(self.get_cph, dates), total=len(dates))
            )
        return pd.concat(cph_dfs, ignore_index=True)

    def get_fees(self, extra) -> pd.DataFrame:
        api_call = self.api.fees
        api_call._store["base_url"] += "calculated-fees/"
        response = api_call.get(extra=extra)
        if response:
            return pd.DataFrame(response["results"])  # type: ignore[no-any-return]
        else:
            raise NoResponseError("Request returned no result!")
        return pd.DataFrame()

    def get_calculated_fees(self) -> pd.DataFrame:
        batch_size = 1_000
        fields = None
        api_call = self.api.fees
        api_call._store["base_url"] += "calculated-fees/"
        try:
            response = api_call.get(extra=f"limit={1}")
            total_entries = response["count"]
            print(f"Total number of entries: {total_entries}")
        except:
            raise NoResponseError("Request returned no result!")

        total_batches = ceil(total_entries / batch_size)
        extras = [
            f"limit={batch_size}&offset={i * batch_size}"
            for i in range(total_batches)
        ]
        if fields:
            extras = [
                f"{extra}&fields={','.join(fields)}" for extra in extras
            ]

        # worker = partial(self.get_data, data_type)

        with Pool() as pool:
            results = list(
                tqdm(pool.imap(self.get_fees, extras), total=total_batches)
            )

        final_df = pd.DataFrame(pd.concat(results, ignore_index=True))
        return final_df

    def after_login(self):
        # df = self.get_all_cph()
        df = self.get_calculated_fees()
        df.to_csv(f"data/outputs/cph_test.csv", index=False)


if __name__ == "__main__":
    DownloadCPH().main()
