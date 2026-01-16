import pandas as pd
import tomllib
from fetools.scripts.base_main import ReportGeneric
from multiprocess import Pool
from tqdm import tqdm
from pprint import pprint


class ComplianceReport(ReportGeneric):
    # region Initialization
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
        self.mandates: pd.DataFrame = pd.DataFrame()

    # endregion Initialization

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
        # Start with fixed metrics
        base_metrics = [
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
                "slug": "s-instrument-name",
                "contribution_dimension": None,
                "contribution_dimension_2": None,
                "column_title": "Security Name",
            },
        ]

        # Add dynamic guideline metrics from config
        guidelines = self.config.get("guidelines", {})
        for _, guideline_config in guidelines.items():
            base_metrics.append(
                {
                    "order": len(base_metrics),
                    "slug": guideline_config["slug"],
                    "contribution_dimension": None,
                    "contribution_dimension_2": None,
                    "column_title": guideline_config["Entry name"],
                }
            )

        # Add dynamic return metrics from config
        returns = self.config.get("returns", [])
        for return_metric in returns:
            base_metrics.append(
                {
                    "order": len(base_metrics),
                    "slug": return_metric["slug"],
                    "contribution_dimension": None,
                    "contribution_dimension_2": None,
                    "column_title": return_metric["name"],
                }
            )

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
            "metrics": {"selected": base_metrics},
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
            guidelines = pd.read_csv(
                s3_path, dtype={"Client": str, "Comparison Value": str}
            )
            guidelines = guidelines[guidelines["Comparison Value"].notna()]
            guidelines = guidelines[
                [
                    "Entity",
                    "FPK",
                    "Client",
                    "Investment Guideline Grouping",
                    "Comparison Value",
                    "Lower Limit",
                    "Upper Limit",
                    "Limit Tolerance",
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
            mappers = []
            for guideline, mapping in self.config["mappers"].items():
                mapping_rules = pd.DataFrame(
                    mapping.items(), columns=["Comparison", "Compliance Item"]
                )
                mapping_rules["Level"] = guideline
                mappers.append(mapping_rules)
            mapping_df = pd.concat(mappers, ignore_index=True)
            guidelines = guidelines.merge(
                mapping_df, how="left", on=["Level", "Comparison"]
            )

            guidelines = guidelines.sort_values(
                ["Mandate ID", "Level", "Compliance Item"]
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
                    "Compliance Item",
                    "Lower Limit",
                    "Upper Limit",
                    "Limit Tolerance",
                ]
            ]
            guidelines.loc[:, "Lower Limit"] = guidelines[
                "Lower Limit"
            ].fillna(0)
            guidelines.loc[:, "Upper Limit"] = guidelines[
                "Upper Limit"
            ].fillna(1)
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
        res = [df for df in res if not df.empty]
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
    def format_mandate_data_frame(self) -> None:
        self.filter_zero_market_value_mandates()
        self.filter_empty_holdings()
        self.add_mandate_returns()
        self.adjust_rows_and_columns()
        self.add_cashlike_definitions()
        self.add_non_classified_label()

    def filter_zero_market_value_mandates(self) -> None:
        main_mandates = (
            self.mandates[self.mandates["Name"] == "Total"][
                ["entity_id", "Mandate MV"]
            ]
            .fillna(0)
            .reset_index(drop=True)
        )
        zero_mandates = main_mandates[
            (main_mandates["Mandate MV"] >= -1)
            & (main_mandates["Mandate MV"] <= 1)
        ]["entity_id"].tolist()
        self.mandates = self.mandates[
            ~self.mandates["entity_id"].isin(zero_mandates)
        ].reset_index(drop=True)

    def filter_empty_holdings(self) -> None:
        self.mandates = self.mandates[
            ~self.mandates["Market Value"].isna()
        ].reset_index(drop=True)
        self.mandates = self.mandates[
            (self.mandates["Market Value"] >= 0.01)
            | (self.mandates["Market Value"] <= -0.01)
        ].reset_index(drop=True)

    def add_mandate_returns(self) -> None:
        returns = self.config.get("returns", [])
        if not returns:
            return

        # Get return column names from config
        return_cols = [return_metric["name"] for return_metric in returns]

        # Extract returns for "Total" rows
        mandate_returns = self.mandates.loc[
            self.mandates["Name"] == "Total",
            ["entity_id"] + return_cols,
        ].reset_index(drop=True)

        # Drop return columns from main df and merge back
        self.mandates = self.mandates.drop(columns=return_cols).merge(
            mandate_returns, how="left"
        )

    def adjust_rows_and_columns(self) -> None:
        self.mandates = self.mandates[
            self.mandates["Name"] != "Total"
        ].reset_index(drop=True)

        # Rename fixed columns
        rename_map = {
            "Name": "Instrument ID",
            "Security Name": "Instrument Name",
            "Security Currency": "Currency",
        }
        self.mandates = self.mandates.rename(columns=rename_map)

        # Ensure Instrument ID is string type
        self.mandates["Instrument ID"] = self.mandates[
            "Instrument ID"
        ].astype(str)

    def add_cashlike_definitions(self) -> None:
        # Find guidelines with Cash classification defined
        guidelines = self.config.get("guidelines", {})
        for _, guideline_config in guidelines.items():
            if "Cash classification" in guideline_config:
                col_name = guideline_config["Entry name"]
                cash_value = guideline_config["Cash classification"]
                self.mandates.loc[
                    self.mandates["Currency"]
                    == self.mandates["Instrument ID"],
                    col_name,
                ] = cash_value

    def add_non_classified_label(self) -> None:
        guidelines = self.config.get("guidelines", {})
        for _, guideline_config in guidelines.items():
            col_name = guideline_config["Entry name"]
            self.mandates.loc[self.mandates[col_name].isna(), col_name] = (
                "Non-Classified"
            )

    # endregion Data formatting

    # region Check compliance

    def check_compliance_rules(self) -> pd.DataFrame:
        # check guidelines
        guideline_checks = self.check_all_guidelines()
        guideline_checks.to_csv(
            "data/outputs/compliance/guideline_checks.csv", index=False
        )
        # check short and leverage
        # check concentration
        # add breaches
        return pd.DataFrame()

    def check_all_guidelines(self) -> pd.DataFrame:
        results = []
        for guideline in self.config["guidelines"].keys():
            results.append(self.check_guideline(guideline))
        return pd.concat(results, ignore_index=True)

    def check_guideline(self, guideline: str) -> pd.DataFrame:
        guideline_column = self.config["guidelines"][guideline]["Entry name"]
        guideline_limits = self.guidelines[
            self.guidelines["Level"] == guideline
        ]
        guideline_values = self.mandates[
            ["Mandate ID", guideline_column, "Market Value"]
        ].copy()
        guideline_values = (
            guideline_values.groupby(["Mandate ID", guideline_column])
            .sum()
            .reset_index()
        )
        mandate_values = self.mandates.copy()[
            ["Mandate ID", "Mandate MV"]
        ].drop_duplicates()
        guideline_values = guideline_values.merge(
            mandate_values, how="left", on="Mandate ID"
        )
        guideline_values["Current Weight"] = (
            guideline_values["Market Value"] / guideline_values["Mandate MV"]
        )
        guideline_values = guideline_values.rename(
            columns={guideline_column: "Compliance Item"}
        )
        guideline_values = (
            guideline_values.merge(
                guideline_limits[
                    [
                        "Mandate ID",
                        "Compliance Item",
                        "Lower Limit",
                        "Upper Limit",
                        "Limit Tolerance",
                    ]
                ],
                how="left",
            )
            .drop_duplicates()
            .reset_index(drop=True)
        )
        guideline_values["Compliance Rule"] = guideline_column

        return guideline_values

    # endregion Check compliance

    def after_login(self) -> None:
        entity_ids = self.guidelines["entity_id"].unique().tolist()
        # Don't worry about these copilot. These rows are here just to speed up the process while I test.
        # Final version will download all mandates, and there'll be no hardcoded file read.
        # self.mandates = self.get_all_mandates(entity_ids)
        # self.mandates.to_parquet(
        #     "data/outputs/compliance/mandates.parquet", index=False
        # )
        self.mandates = pd.read_parquet(
            "data/outputs/compliance/mandates.parquet"
        )
        self.format_mandate_data_frame()
        self.mandates.to_parquet(
            "data/outputs/compliance/mandates_formatted.parquet",
            index=False,
        )
        self.mandates.to_csv(
            "data/outputs/compliance/mandates_formatted.csv", index=False
        )
        self.guidelines.to_csv(
            "data/outputs/compliance/guidelines.csv", index=False
        )
        self.check_compliance_rules()


if __name__ == "__main__":
    crm = ComplianceReport(
        config_file="data/inputs/compliance/compliance.toml"
    )
    crm.main()  # This initializes the API and logs in
