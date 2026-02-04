import logging
import sys
import pandas as pd
from datetime import datetime
from typing import Optional, Any
import json
from pprint import pprint

from fetools.api.base_main import BaseMain, D1g1tApi
from fetools.utils.exceptions import NoResponseError
from fetools.utils.d1g1tparser import ChartTableFormatter

LOG = logging.getLogger(__name__)


class TransferValuesWizard(BaseMain):
    def __init__(self):
        super().__init__()
        self._transaction_payload: dict = {}
        self._entity_id: str = ""

    def add_extra_args(self):
        """
        No extra CLI args needed for the wizard itself,
        as we will prompt the user interactively.
        However, we can keep the base ones for convenience if they want to
        pre-fill via CLI, but the request asks for a wizard.
        """
        pass

    @property
    def entity_id(self) -> str | Any:
        if not self._entity_id:
            api_call = self.api.data
            api_call._store[
                "base_url"
            ] += f"{self.entity_level}/{self.firm_provided_key}"
            response = api_call.get()
            self._entity_id = response.get("entity_id", "")
        return self._entity_id

    @property
    def transaction_payload(self) -> dict:
        """
        Override to prevent usage of the standard transaction payload.
        The wizard will build its own payload.
        """
        if not self._transaction_payload:
            if self.entity_level == "accounts":
                entity = {"accounts_or_positions": [[self.entity_id]]}
            elif self.entity_level == "clients":
                entity = {"clients": [self.entity_id]}
            else:  # households
                entity = {"households": [self.entity_id]}

            transaction_payload = self.load_json_template(
                "data/templates/payloads/transactions.json"
            )
            transaction_payload["options"]["date_range"][
                "start_date"
            ] = self.start_date
            transaction_payload["options"]["date_range"][
                "end_date"
            ] = self.end_date
            transaction_payload["settings"]["currency"] = self.currency
            transaction_payload["settings"]["date"]["date"] = self.end_date
            transaction_payload["control"]["selected_entities"] = entity
            self._transaction_payload = transaction_payload
        return self._transaction_payload

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

    def clean_url(self, url: str) -> str:
        return url.split("/")[-2]

    def load_json_template(self, path: str) -> dict | Any:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

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
            try:
                datetime.strptime(val, "%Y-%m-%d")
                return val
            except ValueError:
                print("Invalid date format. Please use YYYY-MM-DD.")

    def compute_transfer_values(self):
        transactions = self.get_transactions()
        self.add_market_prices(transactions)

    def get_transactions(self) -> pd.DataFrame:
        payload = self.transaction_payload
        calc_call = self.api.calc("log-details-single")
        response = calc_call.post(data=payload)
        if not response:
            raise NoResponseError("No response received for transactions.")
        parser = ChartTableFormatter(response, payload)
        res = parser.parse_data()
        res = res[res["Transaction Type"].isin(self.transaction_types)]
        if self.instrument_id:
            res = res[res["Security ID"] == self.instrument_id]
        return res

    def add_market_prices(self, trx: pd.DataFrame) -> pd.DataFrame:
        instrument_ids = trx["Security ID"].unique().tolist()
        api_call = self.api.data.marketprices
        pprint(api_call._store["base_url"])
        price_list = []
        for instrument in instrument_ids:
            response = api_call.get(extra="limit=1000")
            res = pd.DataFrame(response["results"])
            price_list.append(res)
        prices = pd.concat(price_list, ignore_index=True)
        prices["instrument"] = [
            self.clean_url(url) for url in prices["instrument"]
        ]
        pprint(prices)
        return prices

    def run_wizard(self):
        print("--- Transfer Values Wizard ---")

        # Entity Info
        self.entity_level = self._prompt_choice(
            "Entity Level", ["accounts", "clients", "households"]
        )
        self.firm_provided_key = self._prompt("Entity ID")
        instrument_id_raw = self._prompt("Instrument ID (optional)")
        self.instrument_id = instrument_id_raw if instrument_id_raw else None
        self.currency = self._prompt_choice("Currency", ["CAD", "USD"])

        # Dates
        self.start_date = self._prompt_date("Start Date (YYYY-MM-DD)")
        self.end_date = self._prompt_date(
            "End Date (YYYY-MM-DD, leave blank for single day)",
            self.start_date,
        )

        # Basic Info if not provided via CLI
        if not self.args.server:
            self.args.server = self._prompt(
                "API Server (e.g. api.example.com)"
            )
        if not self.args.username:
            self.args.username = self._prompt("Username")

        # Login
        self.domain = self.get_domain()
        self.options["DOMAIN"] = self.domain
        self.api = D1g1tApi(self.options)

        print(f"\nLogging into {self.domain}...")
        if self.login():
            self.compute_transfer_values()
        else:
            print("Login failed. Exiting.")
            sys.exit(1)

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
