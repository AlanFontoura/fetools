import pandas as pd
import tomllib
from fetools.scripts.base_main import ReportGeneric
from multiprocess import Pool
from tqdm import tqdm
from pprint import pprint


class ComplianceReport(ReportGeneric):
    def __init__(self, config_file: str = ""):
        with open(config_file, "rb") as f:
            self.config = tomllib.load(f)

        super().__init__()

        # Override parsed args with config values
        assert self.args is not None, "Arguments not initialized"
        self.args.username = self.config.get("username")
        self.args.server = self.config.get("server")

        self._guidelines: pd.DataFrame = pd.DataFrame()
        self._risk_profiles: pd.DataFrame = pd.DataFrame()

    # region Payloads
    @property
    def risk_profiles_payload(self) -> dict:
        return {
            "options": {
                "single_result": False,
                "order_by": ["-m-mandate-firm-provided-key"],
            },
            "control": {
                "filter": {
                    "url": "__all_clients__",
                    "name": "All clients",
                    "rank": None,
                    "join_operator": None,
                    "filter_items": None,
                }
            },
            "settings": {
                "currency": self.config.get("currency"),
                "date": {
                    "date": self.config.get("report_date"),
                    "value": "specificDate",
                },
            },
            "groups": {
                "selected": [
                    {
                        "grouping_criterion": f"https://{self.config.get('server')}/api/v1/constants/groupings/account-mandate/",
                        "order": 0,
                    }
                ]
            },
            "metrics": {
                "selected": [
                    {
                        "order": 0,
                        "slug": "m-client-name",
                        "contribution_dimension": None,
                        "contribution_dimension_2": None,
                        "column_title": "Client Name",
                    },
                    {
                        "order": 1,
                        "slug": "m-mandate-firm-provided-key",
                        "contribution_dimension": None,
                        "contribution_dimension_2": None,
                        "column_title": "Mandate ID",
                    },
                    {
                        "order": 2,
                        "slug": "m-mandate-risk-profile-name",
                        "contribution_dimension": None,
                        "contribution_dimension_2": None,
                        "column_title": "Risk Profile",
                    },
                ]
            },
        }

    @property
    def mandate_payload(self) -> dict:
        return {
            "options": {
                "single_result": True,
                "show_multiple_positions": True,
                "order_by": [],
            },
            "control": {"selected_entities": {"investment_mandates": None}},
            "settings": {
                "currency": self.config.get("currency"),
                "date": {
                    "date": self.config.get("report_date"),
                    "value": "specificDate",
                },
            },
            "groups": {
                "selected": [
                    {
                        "grouping_criterion": f"https://{self.config.get('server')}/api/v1/constants/groupings/security-id/",
                        "order": 0,
                    }
                ]
            },
            "metrics": {
                "selected": [
                    {
                        "order": 0,
                        "slug": "q-spot-holding-values-value",
                        "contribution_dimension": None,
                        "contribution_dimension_2": None,
                        "column_title": "Market Value",
                    },
                    {
                        "order": 1,
                        "slug": "s-instrument-currency-name",
                        "contribution_dimension": None,
                        "contribution_dimension_2": None,
                        "column_title": "Security Currency",
                    },
                    {
                        "order": 2,
                        "slug": "s-instrument-asset-class-name",
                        "contribution_dimension": None,
                        "contribution_dimension_2": None,
                        "column_title": "Security Asset Class",
                    },
                    {
                        "order": 3,
                        "slug": "s-instrument-asset-class-l2-name",
                        "contribution_dimension": None,
                        "contribution_dimension_2": None,
                        "column_title": "Security Asset Sub-class",
                    },
                    {
                        "order": 4,
                        "slug": "s-instrument-sub-strategy",
                        "contribution_dimension": None,
                        "contribution_dimension_2": None,
                        "column_title": "Sub Strategy",
                    },
                    {
                        "order": 5,
                        "slug": "s-strategy",
                        "contribution_dimension": None,
                        "contribution_dimension_2": None,
                        "column_title": "Strategy",
                    },
                    {
                        "order": 6,
                        "slug": "q-interval-irr-mwr-values-annualized-extern-invest-irr-last-quarter",
                        "contribution_dimension": None,
                        "contribution_dimension_2": None,
                        "column_title": "Internal Rate of Return (1Q)",
                    },
                    {
                        "order": 7,
                        "slug": "q-interval-irr-mwr-values-annualized-extern-invest-irr-last-year",
                        "contribution_dimension": None,
                        "contribution_dimension_2": None,
                        "column_title": "Internal Rate of Return (1Y)",
                    },
                    {
                        "order": 8,
                        "slug": "s-instrument-name",
                        "contribution_dimension": None,
                        "contribution_dimension_2": None,
                        "column_title": "Security Name",
                    },
                ]
            },
        }

    @property
    def account_payload(self) -> dict:
        return {
            "options": {
                "single_result": True,
                "order_by": ["-q-spot-holding-values-value"],
            },
            "control": {"selected_entities": {"investment_mandates": None}},
            "settings": {
                "currency": self.config.get("currency"),
                "date": {
                    "date": self.config.get("report_date"),
                    "value": "specificDate",
                },
            },
            "groups": {
                "selected": [
                    {
                        "grouping_criterion": f"https://{self.config.get('server')}/api/v1/constants/groupings/account-id/",
                        "order": 0,
                    }
                ]
            },
            "metrics": {
                "selected": [
                    {
                        "order": 0,
                        "slug": "a-entity-account-client-id",
                        "contribution_dimension": None,
                        "contribution_dimension_2": None,
                        "column_title": "Client ID",
                    },
                    {
                        "order": 1,
                        "slug": "a-entity-account-client-name",
                        "contribution_dimension": None,
                        "contribution_dimension_2": None,
                        "column_title": "Client",
                    },
                    {
                        "order": 2,
                        "slug": "a-entity-account-rep-code-rep-code",
                        "contribution_dimension": None,
                        "contribution_dimension_2": None,
                        "column_title": "Rep Code",
                    },
                    {
                        "order": 3,
                        "slug": "a-investment-mandate-firm-provided-key",
                        "contribution_dimension": None,
                        "contribution_dimension_2": None,
                        "column_title": "Mandate ID",
                    },
                    {
                        "order": 4,
                        "slug": "a-investment-mandate-id",
                        "contribution_dimension": None,
                        "contribution_dimension_2": None,
                        "column_title": "Account Mandate Entity ID",
                    },
                    {
                        "order": 5,
                        "slug": "q-spot-holding-values-value",
                        "contribution_dimension": None,
                        "contribution_dimension_2": None,
                        "column_title": "Market Value",
                    },
                ]
            },
        }

    # endregion Payloads

    # region Data downloaders
    @property
    def guidelines(self) -> pd.DataFrame:
        s3_path = f"s3://d1g1t-client-{self.config['region']}/{self.config['client']}/exports/{self.config['env']}-{self.config['client']}-investment-mandates-guideline-limits.csv"
        if self._guidelines.empty:
            guidelines = pd.read_csv(s3_path, dtype={"Client": str})
            guidelines = guidelines[
                [
                    "Entity",
                    "FPK",
                    "Client",
                    "Investment Guideline Grouping",
                    "Comparison Value",
                    "Lower Limit",
                    "Upper Limit",
                ]
            ]
            guidelines = guidelines.rename(
                columns={
                    "Entity": "entity_id",
                    "FPK": "Mandate ID",
                    "Client": "Client ID",
                    "Investment Guideline Grouping": "Level",
                    "Comparison Value": "Comparison",
                }
            )
            guidelines = guidelines.sort_values(
                ["Mandate ID", "Level", "Comparison"]
            ).reset_index(drop=True)

            guidelines = guidelines.merge(self.risk_profiles, how="left")
            guidelines = guidelines[
                [
                    "entity_id",
                    "Mandate ID",
                    "Risk Profile",
                    "Client ID",
                    "Client Name",
                    "Level",
                    "Comparison",
                    "Lower Limit",
                    "Upper Limit",
                ]
            ]
            self._guidelines = guidelines
        return self._guidelines

    @property
    def risk_profiles(self) -> pd.DataFrame:
        if self._risk_profiles.empty:
            risk_profiles = self.get_calculation(
                "track-mandates", self.risk_profiles_payload
            )
            risk_profiles = risk_profiles[risk_profiles["Name"] != "Total"]
            risk_profiles = risk_profiles[
                ["Mandate ID", "Client Name", "Risk Profile"]
            ]
            self._risk_profiles = (
                risk_profiles.copy()
                .sort_values(["Client Name", "Mandate ID", "Risk Profile"])
                .reset_index(drop=True)
            )
        return self._risk_profiles

    def get_all_mandates(self, entity_ids: list[str]) -> pd.DataFrame:
        print("Downloading mandate data...")
        print(f"Total mandates to process: {len(entity_ids)}")
        with Pool() as pool:
            res = list(
                tqdm(
                    pool.imap(self.get_mandate_data, entity_ids),
                    total=len(entity_ids),
                )
            )
        combined_df: pd.DataFrame = pd.concat(res, ignore_index=True)
        return combined_df

    def get_mandate_data(self, entity_id: str) -> pd.DataFrame:
        try:
            holdings = self.get_mandate_holdings(entity_id)
            main_data = self.get_account_level_data(entity_id)
            if not main_data.empty:
                return holdings.merge(main_data, how="left")
            return pd.DataFrame()
        except Exception as e:
            return pd.DataFrame()

    def get_mandate_holdings(self, entity_id: str) -> pd.DataFrame:
        payload = self.mandate_payload
        payload["control"]["selected_entities"]["investment_mandates"] = [
            entity_id
        ]
        response = self.get_calculation("cph-table", payload)
        if response.empty:
            raise Exception(f"No response for entity ID {entity_id}")
        response["entity_id"] = entity_id
        return response

    def get_account_level_data(self, entity_id: str) -> pd.DataFrame:
        payload = self.account_payload
        payload["control"]["selected_entities"]["investment_mandates"] = [
            entity_id
        ]
        response = self.get_calculation("cph-table", payload)
        if response.empty:
            raise Exception(f"No response for entity ID {entity_id}")
        res = self.get_main_data(response)
        return res

    def get_main_data(self, account_level_df: pd.DataFrame) -> pd.DataFrame:
        if account_level_df.loc[0, "Market Value"] is None:
            return pd.DataFrame()
        main_data = {
            "Mandate MV": account_level_df.loc[0, "Market Value"],
            "entity_id": account_level_df.loc[1, "Account Mandate Entity ID"],
            "Mandate ID": account_level_df.loc[1, "Mandate ID"],
        }
        basic_df = account_level_df[account_level_df["Name"] != "Total"]
        for col in ["Client", "Client ID", "Rep Code"]:
            df = (
                basic_df[[col, "Market Value"]]
                .groupby(col)
                .sum()
                .sort_values("Market Value", ascending=False)
                .reset_index()
            )
            main_data[col] = df.loc[0, col]
        return pd.DataFrame([main_data])

    # endregion Data downloaders

    # region Data formatting

    def after_login(self) -> None:
        entity_ids = self.guidelines["entity_id"].unique().tolist()
        data = self.get_all_mandates(entity_ids)
        pprint(data.head())
        pprint(data.shape)


if __name__ == "__main__":
    crm = ComplianceReport(
        config_file="data/inputs/compliance/compliance.toml"
    )
    crm.main()  # This initializes the API and logs in
