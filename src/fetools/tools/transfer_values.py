import logging
import sys
import pandas as pd
from datetime import datetime
from typing import Optional, Any
import json
from dataclasses import dataclass
from multiprocess import Pool
from tqdm import tqdm
from pprint import pprint

from fetools.api.base_main import BaseMain, D1g1tApi
from fetools.utils.exceptions import NoResponseError
from fetools.utils.d1g1tparser import ChartTableFormatter
from math import ceil

LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class TransferValueSettings:
    entity_level: str
    firm_provided_key: str
    instrument_id: Optional[str]
    report_currency: str
    start_date: Optional[str]
    end_date: str
    server: str
    username: str


class TransferValueDownloader(BaseMain):
    # region Initialization and helpers
    def __init__(self, settings: TransferValueSettings) -> None:
        super().__init__()
        self.settings: TransferValueSettings = settings
        self.args.server: str = settings.server
        self.args.username: str = settings.username

    def clean_url(self, url: str) -> str:
        return url.split("/")[-2]

    def load_json_template(self, path: str) -> dict | Any:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    # endregion

    # region Transaction downloaders
    @property
    def entity_id(self) -> str | Any:
        api_call = self.api.data
        api_call._store[
            "base_url"
        ] += (
            f"{self.settings.entity_level}/{self.settings.firm_provided_key}/"
        )
        response = api_call.get()
        return response.get("entity_id", "")

    @property
    def transaction_types(self) -> list[str]:
        return [
            "Transfer Cash In",
            "Transfer Cash Out",
            "Internal Transfer Cash In",
            "Internal Transfer Cash Out",
            "Transfer Security In",
            "Transfer Security Out",
            "Internal Transfer Security In",
            "Internal Transfer Security Out",
            "Deposit",
            "Withdrawal",
        ]

    def transaction_payload(self) -> dict:
        if self.settings.entity_level == "accounts":
            entity = {"accounts_or_positions": [[self.entity_id]]}
        elif self.settings.entity_level == "clients":
            entity = {"clients": [self.entity_id]}
        else:  # households
            entity = {"households": [self.entity_id]}

        if self.settings.start_date is None:
            date_range = {
                "value": "since_inception",
                "label": "Since Inception",
            }
        else:
            date_range = {
                "value": "custom",
                "label": "Custom",
                "start_date": self.settings.start_date,
                "end_date": self.settings.end_date,
            }

        transaction_payload = self.load_json_template(
            "data/templates/payloads/transactions.json"
        )
        transaction_payload["options"]["date_range"] = date_range
        transaction_payload["settings"][
            "currency"
        ] = self.settings.report_currency
        transaction_payload["settings"]["date"][
            "date"
        ] = self.settings.end_date
        transaction_payload["control"]["selected_entities"] = entity

        return transaction_payload

    def get_transactions(self) -> pd.DataFrame:
        payload = self.transaction_payload()
        calc_call = self.api.calc("log-details-single")
        response = calc_call.post(data=payload)
        if not response:
            raise NoResponseError("No response received for transactions.")
        parser = ChartTableFormatter(response, payload)
        res = parser.parse_data()
        res = res[res["Transaction Type"].isin(self.transaction_types)]
        res = res[~res["Is Cancelled"]]
        if self.settings.instrument_id:
            res = res[res["Security ID"] == self.settings.instrument_id]
        return res

    # endregion

    # region Market Price downloaders
    def fill_missing_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        dates = pd.date_range(start=df["date"].min(), end=df["date"].max())
        all_dates = [d.strftime("%Y-%m-%d") for d in dates]
        all_dates_df = pd.DataFrame(all_dates, columns=["date"])
        merged_df = all_dates_df.merge(df, on="date", how="left").sort_values(
            "date"
        )
        merged_df = merged_df.ffill()
        return merged_df

    def get_market_price_history(self, instrument_id: str) -> pd.DataFrame:
        market_price_call = self.api.marketprices
        response = market_price_call.get(
            extra=f"instrument={instrument_id}&limit=9999&fields=instrument_key,date,close,source"
        )
        res = pd.DataFrame(response["results"])
        res = res[res["source"] == "C"].drop(columns=["source"])
        return self.fill_missing_dates(res)

    def get_market_prices(self, transactions: pd.DataFrame) -> pd.DataFrame:
        instruments = [
            instrument
            for instrument in transactions["Security ID"].unique()
            if instrument is not None
        ]
        if not instruments:
            return pd.DataFrame()
        if len(instruments) == 1:
            prices = self.get_market_price_history(instruments[0])
            return prices

        with Pool() as pool:
            results = list(
                tqdm(
                    pool.imap(self.get_market_price_history, instruments),
                    total=len(instruments),
                    desc=f"Fetching market prices for {len(instruments)} instruments",
                )
            )
        market_prices = pd.DataFrame(pd.concat(results, ignore_index=True))
        return market_prices

    # endregion

    # region FX downloaders
    def get_fx_batch(self, extra: str) -> pd.DataFrame:
        fx_call = self.api.fxrates
        response = fx_call.get(extra=extra)
        res = pd.DataFrame(response["results"])
        return res

    def get_fx_rates(self, batch_size: int = 1_000) -> pd.DataFrame:
        LOG.info("Fetching FX rates...")
        fx_call = self.api.fxrates
        total = fx_call.get(extra="limit=1")["count"]
        if total == 0:
            LOG.warning("No FX rate data available.")
            res = pd.DataFrame(
                columns=["base_name", "foreign_name", "date", "close"]
            )
        elif total < batch_size:
            res = self.get_fx_batch(
                extra=f"limit={total}&fields=base_name,foreign_name,date,close"
            )
        else:
            extras = [
                f"limit={batch_size}&offset={i * batch_size}&fields=base_name,foreign_name,date,close"
                for i in range(ceil(total / batch_size))
            ]
            with Pool() as pool:
                results = list(
                    tqdm(
                        pool.imap(self.get_fx_batch, extras),
                        total=len(extras),
                        desc="Fetching FX rates in batches",
                    )
                )
            res = pd.DataFrame(pd.concat(results, ignore_index=True))
        res = self.format_fx_rates(res)
        return res

    def format_fx_rates(self, fx_rates: pd.DataFrame) -> pd.DataFrame:
        fx_rates = fx_rates.sort_values(
            by=["base_name", "foreign_name", "date"]
        )
        fx_rates = (
            fx_rates.groupby(["base_name", "foreign_name"])
            .apply(self.fill_missing_dates)
            .reset_index(drop=True)
        )
        return fx_rates

    # endregion
    def after_login(self):
        trx = self.get_transactions()
        if trx.empty:
            print(
                "No valid transfer transactions found for the given criteria."
            )
            return
        market_prices = self.get_market_prices(trx)
        fx_rates = self.get_fx_rates()
        fx_rates.to_csv("fx_rates.csv", index=False)


class TransferValuesWizard:
    def _prompt(self, message: str, default: Optional[str] = None) -> str:
        prompt_text = (
            f"{message} [{default}]: " if default else f"{message}: "
        )
        val = input(prompt_text).strip()
        return val if val else (default if default else "")

    def _prompt_choice(self, message: str, choices: list[str]) -> str:
        print(f"\nSelect {message}:")
        for i, choice in enumerate(choices, 1):
            print(f"  {i}. {choice}")

        while True:
            val = input(f"Enter choice number [1-{len(choices)}]: ").strip()
            if val.isdigit():
                idx = int(val) - 1
                if 0 <= idx < len(choices):
                    return choices[idx]
            print(
                f"Invalid selection. Please enter a number between 1 and {len(choices)}."
            )

    def _prompt_date(
        self, message: str, default: Optional[str] = None
    ) -> str:
        while True:
            val = self._prompt(message, default)
            if val == "" and default is not None:
                return default
            try:
                datetime.strptime(val, "%Y-%m-%d")
                return val
            except ValueError:
                print("Invalid date format. Please use YYYY-MM-DD.")

    def run_wizard(self):
        print("--- Transfer Values Wizard ---")

        # Entity Info
        # entity_level = self._prompt_choice(
        #     "Entity Level", ["accounts", "clients", "households"]
        # )
        # firm_provided_key = self._prompt("Entity ID")
        # instrument_id_raw = self._prompt("Instrument ID (optional)")
        # instrument_id = instrument_id_raw if instrument_id_raw else None
        # report_currency = self._prompt_choice("Currency", ["CAD", "USD"])

        # # Dates
        # end_date = self._prompt_date("End Date (YYYY-MM-DD)")
        # start_date = self._prompt_date(
        #     "Start Date (YYYY-MM-DD, leave blank for 'Since Inception')",
        #     default="",
        # )

        # server = self._prompt("API Server (e.g. api.example.com)")
        # username = self._prompt("Username")

        # settings = TransferValueSettings(
        #     entity_level=entity_level,
        #     firm_provided_key=firm_provided_key,
        #     instrument_id=instrument_id if instrument_id else None,
        #     report_currency=report_currency,
        #     start_date=start_date if start_date else None,
        #     end_date=end_date,
        #     server=server,
        #     username=username,
        # )

        # Hardcoded settings for simplicity
        settings = TransferValueSettings(
            entity_level="households",
            firm_provided_key="HH000C74",
            instrument_id=None,
            report_currency="CAD",
            start_date=None,
            end_date="2023-04-30",
            server="api-v52.d1g1tdev.com",
            username="alan.fontoura@d1g1t.com",
        )

        downloader = TransferValueDownloader(settings)
        downloader.main()

    def main(self):
        # Override main to run the wizard instead of the standard flow
        try:
            self.run_wizard()
        except KeyboardInterrupt:
            print("\nOperation cancelled by user.")
            sys.exit(0)


def main():
    wizard = TransferValuesWizard()
    wizard.main()


if __name__ == "__main__":
    main()
