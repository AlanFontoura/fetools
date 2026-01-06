import pandas as pd


# SMA
class SMADataGenerator:
    def __init__(self, sma_df: pd.DataFrame):
        self.sma_df = sma_df

    def generate_sma_data(self) -> None:
        self.create_sma_funds()
        self.create_sma_class_series()
        self.create_sma_households()
        self.create_sma_clients()
        self.create_sma_accounts()
        self.remap_sma_accounts()
        self.create_sma_instruments()
        self.create_sma_fund_client_ownership()

    def create_sma_funds(self) -> None:
        self.funds: pd.DataFrame = self.sma_df[
            ["Account ID", "Account Name", "Currency", "Client ID"]
        ].copy()
        self.funds["Account ID"] = [
            f"{fpk}_sma_fund" for fpk in self.funds["Account ID"]
        ]
        self.funds["Account Name"] = [
            f"{name} - SMA Fund" for name in self.funds["Account Name"]
        ]
        self.funds["Type"] = "SMA"
        self.funds = self.funds.rename(
            columns={
                "Account ID": "Firm Provided Key",
                "Account Name": "Name",
                "Client ID": "Fund Manager Firm Provided Key",
            }
        )

    def create_sma_class_series(self) -> None:
        self.class_series = self.sma_df[["Account ID", "Account Name"]].copy()
        self.class_series["Account ID"] = [
            f"{fpk}_sma_class_series"
            for fpk in self.class_series["Account ID"]
        ]
        self.class_series["Account Name"] = [
            f"{name} - SMA Class Series"
            for name in self.class_series["Account Name"]
        ]
        self.class_series["Weight"] = 1.0
        self.class_series["Fund Firm Provided Key"] = self.class_series[
            "Account ID"
        ].str.replace("_sma_class_series", "_sma_fund")

        # TODO: Add columns below when supported
        # self.class_series["Is Lookthrough Enabled"] = False
        # self.class_series["Collapse when scaling down position"] = True
        self.class_series = self.class_series.rename(
            columns={
                "Account ID": "Firm Provided Key",
                "Account Name": "Name",
            }
        )

    def create_sma_households(self) -> None:
        self.households = self.sma_df[
            ["Account ID", "Account Name", "Team Name"]
        ].copy()
        self.households["Account ID"] = [
            f"{fpk}_sma_household" for fpk in self.households["Account ID"]
        ]
        self.households["Account Name"] = [
            f"{name} - SMA Household"
            for name in self.households["Account Name"]
        ]
        self.households = self.households.rename(
            columns={
                "Account ID": "Household ID",
                "Account Name": "Household Name",
            }
        )

    def create_sma_clients(self) -> None:
        self.clients = self.sma_df[
            [
                "Account ID",
                "Account Name",
                "Jurisdiction",
                "TaxResidency",
                "Team Name",
            ]
        ].copy()
        self.clients["Account ID"] = [
            f"{fpk}_sma_client" for fpk in self.clients["Account ID"]
        ]
        self.clients["Account Name"] = [
            f"{name} - SMA Client" for name in self.clients["Account Name"]
        ]
        self.clients["ClientTypeDescription"] = "Foundation"
        self.clients["Household ID"] = self.clients["Account ID"].str.replace(
            "_sma_client", "_sma_household"
        )
        self.clients["CRM_ContactID"] = self.clients[
            "Account ID"
        ].str.replace("_sma_client", "_sma_contact")
        self.clients = self.clients.rename(
            columns={
                "Account ID": "ClientID",
                "Account Name": "ClientName",
            }
        )

    def create_sma_accounts(self) -> None:
        self.accounts = self.sma_df.copy().drop(
            columns=self.sma_df.filter(regex="^Asset").columns
        )
        self.accounts["Account ID"] = [
            f"{fpk}_sma_account" for fpk in self.accounts["Account ID"]
        ]
        self.accounts["Account Name"] = [
            f"{name} - SMA Account" for name in self.accounts["Account Name"]
        ]
        self.accounts["Account Type Name"] = "Other"

    def remap_sma_accounts(self) -> None:
        self.account_remap = self.sma_df[["Account ID"]].copy()
        self.account_remap["Class Series ID"] = [
            f"{fpk}_sma_class_series"
            for fpk in self.account_remap["Account ID"]
        ]
        self.account_remap["Client ID"] = [
            f"{fpk}_sma_client" for fpk in self.account_remap["Account ID"]
        ]

    def create_sma_instruments(self) -> None:
        classification_cols = self.sma_df.filter(
            regex="^Asset"
        ).columns.tolist()
        instrument_cols = [
            "Instrument ID",
            "Instrument Name",
            "Currency Name",
            "Valuation Per Position",
            "Firm Security Type Name",
            "Class Series ID",
        ]
        self.instruments = self.sma_df.copy()
        self.instruments["Instrument ID"] = [
            f"{fpk}_sma_instrument" for fpk in self.instruments["Account ID"]
        ]
        self.instruments["Instrument Name"] = [
            f"{name} - SMA Instrument"
            for name in self.instruments["Account Name"]
        ]
        self.instruments["Class Series ID"] = [
            f"{fpk}_sma_class_series"
            for fpk in self.instruments["Account ID"]
        ]
        self.instruments["Firm Security Type Name"] = "Unitless"
        self.instruments["Valuation Per Position"] = True
        self.instruments["Currency Name"] = self.instruments["Currency"]
        self.instruments = self.instruments[
            instrument_cols + classification_cols
        ]

    def create_sma_fund_client_ownership(self) -> None:
        self.fund_client_ownership = self.sma_df[
            ["Account ID", "Date Opened"]
        ].copy()
        self.fund_client_ownership["Class Series ID"] = [
            f"{fpk}_sma_class_series"
            for fpk in self.fund_client_ownership["Account ID"]
        ]
        self.fund_client_ownership["Client Account ID"] = [
            f"{fpk}_sma_account"
            for fpk in self.fund_client_ownership["Account ID"]
        ]
        self.fund_client_ownership["Percent"] = 1.0
        self.fund_client_ownership = self.fund_client_ownership.rename(
            columns={
                "Date Opened": "Start Date",
            }
        )
        self.fund_client_ownership = self.fund_client_ownership.drop(
            columns=["Account ID"]
        )


# Partial Ownership


# Join PO and SMA
