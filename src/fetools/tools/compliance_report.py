import pandas as pd
import tomllib
from fetools.api.base_main import ReportGeneric
from multiprocess import Pool
from tqdm import tqdm
import os
import awswrangler as wr


class ComplianceReport(ReportGeneric):
    # region Initialization
    def __init__(self, config_file: str = ""):
        with open(config_file, "rb") as f:
            self.config = tomllib.load(f)

        super().__init__()

        # Override parsed args with config values
        assert self.args is not None, "Arguments not initialized"
        self.base = self.config.get("base", {})
        self.args.username = self.base.get("username")
        self.args.server = self.base.get("server")

        self._guidelines: pd.DataFrame = pd.DataFrame()
        self._risk_profiles: pd.DataFrame = pd.DataFrame()
        self.mandates: pd.DataFrame = pd.DataFrame()
        self.compliance_checks: pd.DataFrame = pd.DataFrame()
        self.final_report: pd.DataFrame = pd.DataFrame()

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
                "currency": self.base.get("currency"),
                "date": {
                    "date": self.base.get("report_date"),
                    "value": "specificDate",
                },
            },
            "groups": {
                "selected": [
                    {
                        "grouping_criterion": f"https://{self.base.get('server')}/api/v1/constants/groupings/account-mandate/",
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
                    "column_title": guideline_config["name"],
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
                "currency": self.base.get("currency"),
                "date": {
                    "date": self.base.get("report_date"),
                    "value": "specificDate",
                },
            },
            "groups": {
                "selected": [
                    {
                        "grouping_criterion": f"https://{self.base.get('server')}/api/v1/constants/groupings/security-id/",
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
                "currency": self.base.get("currency"),
                "date": {
                    "date": self.base.get("report_date"),
                    "value": "specificDate",
                },
            },
            "groups": {
                "selected": [
                    {
                        "grouping_criterion": f"https://{self.base.get('server')}/api/v1/constants/groupings/account-id/",
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
        s3_path = f"s3://d1g1t-client-{self.base['region']}/{self.base['client']}/exports/{self.base['env']}-{self.base['client']}-investment-mandates-guideline-limits.csv"
        if self._guidelines.empty:
            guidelines = pd.read_csv(
                s3_path, dtype={"Client": str, "Comparison Value": str}
            )
            guidelines = guidelines[guidelines["Comparison Value"].notna()]
            guidelines = guidelines[
                [
                    "Entity",
                    "FPK",
                    "Name",
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
                    "Name": "Mandate Name",
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
                    "Mandate Name",
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

    def get_all_mandates(self, entity_ids: list[str]) -> None:
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
        self.mandates = combined_df

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
                col_name = guideline_config["name"]
                cash_value = guideline_config["Cash classification"]
                self.mandates.loc[
                    self.mandates["Currency"]
                    == self.mandates["Instrument ID"],
                    col_name,
                ] = cash_value

    def add_non_classified_label(self) -> None:
        guidelines = self.config.get("guidelines", {})
        for _, guideline_config in guidelines.items():
            col_name = guideline_config["name"]
            self.mandates.loc[self.mandates[col_name].isna(), col_name] = (
                "Non-Classified"
            )

    # endregion Data formatting

    # region Check compliance

    def check_compliance(self) -> None:
        # Main guideline checks
        guideline_checks = self.check_all_guidelines()

        # Check other compliance rules
        rules = self.config.get("compliance_rules", {})
        if not rules:
            return

        # Short positions
        short = self.check_negative_positions(
            type="short", risk_profiles=rules.get("check_short_positions", [])
        )

        # Leverage
        leverage = self.check_negative_positions(
            type="leverage", risk_profiles=rules.get("check_leverage", [])
        )

        # Concentration
        concentration_checks = self.check_concentration(
            concentration_rules=rules.get("concentration", {})
        )
        # add breaches
        compliance_checks = pd.concat(
            [guideline_checks, short, leverage, concentration_checks],
            ignore_index=True,
        )

        self.compliance_checks = compliance_checks

    def check_all_guidelines(self) -> pd.DataFrame:
        results = []
        for guideline in self.config["guidelines"].keys():
            results.append(self.check_guideline(guideline))
        return pd.concat(results, ignore_index=True)

    def check_guideline(self, guideline: str) -> pd.DataFrame:
        guideline_column = self.config["guidelines"][guideline]["name"]
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
        guideline_values["Current Weight"] = round(
            guideline_values["Market Value"] / guideline_values["Mandate MV"],
            4,
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

    def check_negative_positions(
        self, type: str, risk_profiles: list[str]
    ) -> pd.DataFrame:
        if not risk_profiles or risk_profiles == ["none"]:
            return pd.DataFrame()

        negative_positions = self.mandates.loc[
            self.mandates["Market Value"] < 0,
            [
                "Mandate ID",
                "Instrument ID",
                "Market Value",
                "Mandate MV",
                "Currency",
            ],
        ].copy()

        if type == "short":
            negative_positions = negative_positions[
                negative_positions["Instrument ID"]
                != negative_positions["Currency"]
            ]
            negative_positions["Compliance Rule"] = "Short Position"
        elif type == "leverage":
            negative_positions = negative_positions[
                negative_positions["Instrument ID"]
                == negative_positions["Currency"]
            ]
            negative_positions["Compliance Rule"] = "Leverage"
        else:
            raise ValueError("Type must be either 'short' or 'leverage'")

        negative_positions = negative_positions.rename(
            columns={"Instrument ID": "Compliance Item"}
        )

        negative_positions = negative_positions.drop(columns=["Currency"])

        if risk_profiles == ["all"]:
            return negative_positions

        negative_positions = negative_positions.merge(
            self.risk_profiles[["Mandate ID", "Risk Profile"]],
            how="left",
            on="Mandate ID",
        )
        negative_positions = negative_positions[
            negative_positions["Risk Profile"].isin(risk_profiles)
        ].reset_index(drop=True)

        return negative_positions

    def check_concentration(
        self, concentration_rules: dict[str, float]
    ) -> pd.DataFrame:
        if not concentration_rules or concentration_rules == {"all": 1}:
            return pd.DataFrame()

        if "all" in concentration_rules:
            concentration = self.mandates[
                ["Mandate ID", "Instrument ID", "Market Value", "Mandate MV"]
            ].copy()
            concentration["Current Weight"] = round(
                concentration["Market Value"] / concentration["Mandate MV"], 4
            )
            concentration["Compliance Rule"] = "Concentration"
            concentration = concentration.rename(
                columns={"Instrument ID": "Compliance Item"}
            )
            concentration["Upper Limit"] = concentration_rules["all"]
            concentration = concentration[
                concentration["Current Weight"] > concentration["Upper Limit"]
            ]
            return concentration

        concentration = self.mandates.merge(
            self.risk_profiles[["Mandate ID", "Risk Profile"]], how="left"
        )
        concentration_limits = pd.DataFrame(
            concentration_rules.items(),
            columns=["Risk Profile", "Upper Limit"],
        )
        concentration = concentration.merge(
            concentration_limits, how="left", on="Risk Profile"
        )
        concentration = concentration[concentration["Upper Limit"].notna()]
        concentration["Current Weight"] = round(
            concentration["Market Value"] / concentration["Mandate MV"], 4
        )
        concentration = concentration.loc[
            concentration["Current Weight"] > concentration["Upper Limit"],
            [
                "Mandate ID",
                "Instrument ID",
                "Market Value",
                "Mandate MV",
                "Current Weight",
                "Upper Limit",
            ],
        ].copy()
        concentration["Compliance Rule"] = "Concentration"
        concentration = concentration.rename(
            columns={"Instrument ID": "Compliance Item"}
        )
        return concentration

    # endregion Check compliance

    # region Create report
    def create_report(self) -> None:
        main_info = self.create_main_info()
        self.add_status_to_compliance()
        self.apply_filtering_rules()
        self.create_final_report(main_info)

    def create_main_info(self) -> pd.DataFrame:
        from_guidelines = (
            self.guidelines[
                [
                    "Mandate ID",
                    "Mandate Name",
                    "Risk Profile",
                    "Client ID",
                    "Client Name",
                ]
            ]
            .drop_duplicates()
            .sort_values(["Mandate ID"])
            .reset_index(drop=True)
        )
        selected_returns = self.config.get("returns", [])
        return_cols = [ret["name"] for ret in selected_returns]
        from_mandates = (
            self.mandates[
                ["Mandate ID", "Rep Code", "Mandate MV"] + return_cols
            ]
            .drop_duplicates()
            .sort_values(["Mandate ID"])
            .reset_index(drop=True)
        )
        main_info = from_mandates.merge(
            from_guidelines, how="left", on="Mandate ID"
        )
        return main_info

    def add_status_to_compliance(self) -> None:
        if self.compliance_checks.empty:
            return
        self.compliance_checks["Status"] = "Warning"
        self.compliance_checks["Limit Tolerance"] = self.compliance_checks[
            "Limit Tolerance"
        ].fillna(0)
        self.compliance_checks.loc[
            (
                self.compliance_checks["Current Weight"]
                < self.compliance_checks["Lower Limit"]
            )
            | (
                self.compliance_checks["Current Weight"]
                > self.compliance_checks["Upper Limit"]
            ),
            "Status",
        ] = "Breached"
        self.compliance_checks.loc[
            (
                self.compliance_checks["Current Weight"]
                >= (
                    self.compliance_checks["Lower Limit"]
                    + self.compliance_checks["Limit Tolerance"]
                )
            )
            & (
                self.compliance_checks["Current Weight"]
                <= (
                    self.compliance_checks["Upper Limit"]
                    - self.compliance_checks["Limit Tolerance"]
                )
            ),
            "Status",
        ] = "On Target"
        self.compliance_checks.loc[
            self.compliance_checks["Upper Limit"].isna(), "Status"
        ] = "No Guidelines"
        self.compliance_checks.loc[
            self.compliance_checks["Compliance Rule"].isin(
                ["Leverage", "Short Position"]
            ),
            "Status",
        ] = "Breached"

    def apply_filtering_rules(self) -> None:
        filtering_rules = self.config.get("settings", {})
        if filtering_rules.get("ignore_warnings_near_zero", False):
            self.compliance_checks.loc[
                (self.compliance_checks["Status"] == "Warning")
                & (self.compliance_checks["Lower Limit"] == 0)
                & (
                    self.compliance_checks["Current Weight"]
                    <= self.compliance_checks["Limit Tolerance"]
                ),
                "Status",
            ] = "On Target"

        if filtering_rules.get("ignore_warnings_near_one_hundred", False):
            self.compliance_checks.loc[
                (self.compliance_checks["Status"] == "Warning")
                & (self.compliance_checks["Upper Limit"] == 1)
                & (
                    self.compliance_checks["Current Weight"]
                    >= (1 - self.compliance_checks["Limit Tolerance"])
                ),
                "Status",
            ] = "On Target"

        if filtering_rules.get("hide_compliant_items", False):
            self.compliance_checks = self.compliance_checks[
                self.compliance_checks["Status"] != "On Target"
            ].reset_index(drop=True)

    def create_final_report(self, main_info: pd.DataFrame) -> None:
        return_metrics = self.config.get("returns", [])
        return_cols = [ret["name"] for ret in return_metrics]
        final_report = pd.concat(
            [main_info, self.compliance_checks], ignore_index=True
        )
        cols = (
            [
                "Mandate ID",
                "Mandate Name",
                "Risk Profile",
                "Rep Code",
                "Client ID",
                "Client Name",
            ]
            + return_cols
            + [
                "Mandate MV",
                "Market Value",
                "Compliance Rule",
                "Compliance Item",
                "Status",
                "Current Weight",
                "Lower Limit",
                "Upper Limit",
            ]
        )
        final_report = (
            final_report[cols]
            .sort_values(
                [
                    "Mandate ID",
                    "Client ID",
                    "Compliance Rule",
                    "Compliance Item",
                ]
            )
            .reset_index(drop=True)
        )
        for col in ["Mandate Name", "Risk Profile"]:
            final_report[col] = final_report[col].ffill()
        self.final_report = final_report

    # endregion Create report

    # region Generate excel file
    def export_report(self) -> None:
        file_name = f"Compliance Report - {self.base.get('client').title()} - {self.base.get('report_date')}.xlsx"
        file_path = f"data/outputs/compliance/{file_name}"
        self.create_excel_report(file_path)
        print(f"Report saved to {file_path}")
        if "s3_folder" in self.base:
            wr.s3.upload(
                local_file=file_path,
                path=f"s3://{self.base.get('s3_folder')}/{file_name}",
            )
            print(
                f"Report uploaded to s3://{self.base.get('s3_folder')}/{file_name}"
            )

    def create_excel_report(self, file_path: str) -> None:
        report_date = self.base.get("report_date", "")
        sheetname = f"Compliance Report - {report_date}"
        return_metrics = self.config.get("returns", [])
        return_cols = [ret["name"] for ret in return_metrics]
        with pd.ExcelWriter(file_path, engine="xlsxwriter") as writer:
            self.final_report.to_excel(
                writer,
                sheet_name=sheetname,
                index=False,
            )

            # Creating workbook instance and general cell formats
            workbook = writer.book
            format_pct = workbook.add_format({"num_format": "0.00%"})
            format_pct.set_align("center")
            format_cur = workbook.add_format({"num_format": "#,##0.00"})
            format_center = workbook.add_format()
            format_center.set_align("center")
            format_red = workbook.add_format(
                {"bg_color": "#FFC7CE", "font_color": "#9C0006"}
            )
            format_yellow = workbook.add_format(
                {"bg_color": "#FFEB9C", "font_color": "#9C6500"}
            )
            format_green = workbook.add_format(
                {"bg_color": "#D8E4BC", "font_color": "#00B050"}
            )

            worksheet = writer.sheets[sheetname]
            worksheet.freeze_panes(1, 0)
            worksheet.autofilter(
                0,
                0,
                self.final_report.shape[0],
                self.final_report.shape[1] - 1,
            )

            for col in ["Mandate MV", "Market Value"]:
                col_idx = self.final_report.columns.get_loc(col)
                worksheet.set_column(
                    col_idx,
                    col_idx,
                    15,
                    format_cur,
                )

            for col in return_cols + [
                "Current Weight",
                "Lower Limit",
                "Upper Limit",
            ]:
                col_idx = self.final_report.columns.get_loc(col)
                worksheet.set_column(
                    col_idx,
                    col_idx,
                    15,
                    format_pct,
                )

            status_col_idx = self.final_report.columns.get_loc("Status")
            worksheet.conditional_format(
                1,
                status_col_idx,
                self.final_report.shape[0],
                status_col_idx,
                {
                    "type": "text",
                    "criteria": "containing",
                    "value": "Breached",
                    "format": format_red,
                },
            )
            worksheet.conditional_format(
                1,
                status_col_idx,
                self.final_report.shape[0],
                status_col_idx,
                {
                    "type": "text",
                    "criteria": "containing",
                    "value": "Warning",
                    "format": format_yellow,
                },
            )
            worksheet.conditional_format(
                1,
                status_col_idx,
                self.final_report.shape[0],
                status_col_idx,
                {
                    "type": "text",
                    "criteria": "containing",
                    "value": "On Target",
                    "format": format_green,
                },
            )
            worksheet.set_column(
                status_col_idx, status_col_idx, None, format_center
            )

            worksheet.autofit()

    # endregion Generate excel file

    def after_login(self) -> None:
        entity_ids = self.guidelines["entity_id"].unique().tolist()
        self.get_all_mandates(entity_ids)
        self.format_mandate_data_frame()
        self.check_compliance()
        self.create_report()
        self.export_report()


if __name__ == "__main__":
    compliance = ComplianceReport(
        config_file="data/inputs/compliance/compliance.toml"
    )
    os.makedirs("data/outputs/compliance/", exist_ok=True)
    compliance.main()  # This initializes the API and logs in
