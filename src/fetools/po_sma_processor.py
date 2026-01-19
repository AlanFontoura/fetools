import pandas as pd
from dataclasses import dataclass


@dataclass(frozen=True)
class Account:
    id: str
    name: str
    is_partially_owned: bool = False
    is_SMA: bool = False
    opened_date: str = ""
    advisory_scope: str = ""
    rep_code: str = ""
    currency: str = ""
    udf1: str = ""
    udf2: str = ""
    client_id: str = ""
    client_name: str = ""
    sma_name: str | None = None
    sma_category: str | None = None
    sma_asset_class: str | None = None
    sma_asset_class_l2: str | None = None
    sma_asset_class_l3: str | None = None


@dataclass(frozen=True)
class Owner:
    id: str
    name: str
    ownership_structure: dict[
        str, float
    ]  # date to ownership percentage across dates


class Structure:
    def __init__(self, account: Account, owners: list[Owner]):
        self.account = account
        self.owners = owners

    def create_fund(self):
        return {
            "Firm Provided Key": f"{self.account.id}_fund",
            "Name": f"{self.account.name} - FUND",
            "Currency": self.account.currency,
            "Fund Manager Firm Provided Key": self.account.client_id,
            "Type": "SMA",
        }

    def create_class_series(self):
        return {
            "Firm Provided Key": f"{self.account.id}_class",
            "Fund Firm Provided Key": f"{self.account.id}_fund",
            "Name": f"{self.account.name} - CLASS",
            "Weight": 1,
        }

    def create_household(self):
        return {
            "Household ID": f"{self.account.id}_household",
            "Name": f"{self.account.name} - HOUSEHOLD",
            "Team Name": "ALL CLIENTS",
        }

    def create_client(self):
        return {
            "Client ID": f"{self.account.id}_client",
            "Name": f"{self.account.name} - CLIENT",
            "Household ID": f"{self.account.id}_household",
            "Client Type Description": "Foundation",
            "Jurisdiction": "US",
            "TaxResidency": "US",
            "Team Name": "ALL CLIENTS",
            "CRM_ContactID": f"{self.account.id}_client",
        }

    def update_client(self):
        return {
            "Client ID": self.account.client_id,
            "User Defined 1": (
                "Partially Owned" if self.account.is_partially_owned else None
            ),
        }

    def create_instrument(self):
        return {
            "Instrument ID": f"{self.account.id}_instrument",
            "Name": f"{self.account.name} - INSTRUMENT",
            "Firm Security Type Name": "Unitless",
            "Currency Name": self.account.currency,
            "Class Series ID": f"{self.account.id}_class",
            "Valuation Per Position": True,
            "User Defined 3": "SMA",
            "Asstet Category Name": self.account.sma_category,
            "Asset Class Name": self.account.sma_asset_class,
            "Asset Class l2 Name": self.account.sma_asset_class_l2,
            "Asset Class l3 Name": self.account.sma_asset_class_l3,
        }

    def create_account(self):
        return {
            "Account Type Name": "Other",
            "Account ID": f"{self.account.id}_client",
            "Account Name": self.account.name,
            "Currency Name": self.account.currency,
            "Client ID": self.account.client_id,
            "Date Opened": self.account.opened_date,
            "Inception Date": self.account.opened_date,
            "Advisory Scope Name": self.account.advisory_scope,
            "Rep Code": self.account.rep_code,
            "User Defined 1": self.account.udf1,
            "User Defined 2": self.account.udf2,
            "User Defined 5": "Direct",
        }

    def remap_account(self):
        return {
            "Account ID": self.account.id,
            "Class Series ID": f"{self.account.id}_class",
            "Client ID": f"{self.account.id}_client",
        }

    def fund_client_ownership(self):
        return {
            "Fund Firm Provided Key": f"{self.account.id}_fund",
            "Client ID": f"{self.account.id}_client",
            "Ownership Percentage": 1.0,
        }


class Split:
    def __init__(self, account: Account, owner: Owner):
        self.account = account
        self.owner = owner
        self.most_recent_date = max(owner.ownership_structure.keys())
        self.pct_of_ownership = owner.ownership_structure[
            self.most_recent_date
        ]

    def create_account(self):
        return {
            "Account Type Name": "Other",
            "Account ID": f"{self.account.id}_{self.owner.id}",
            "Account Name": f"{self.owner.name} {self.account.name} - {self.pct_of_ownership:.2%}",
            "Client ID": self.owner.id,
            "Date Opened": self.account.opened_date,
            "Inception Date": self.account.opened_date,
            "Advisory Scope Name": self.account.advisory_scope,
            "Rep Code": self.account.rep_code,
            "User Defined 1": self.account.udf1,
            "User Defined 2": self.account.udf2,
            "User Defined 5": "Split",
        }

    def create_fund_client_ownership(self):
        for date, pct in self.owner.ownership_structure.items():
            yield {
                "Class Series ID": f"{self.account.id}_class",
                "Client Account ID": f"{self.account.id}_client",
                "Date": date,
                "Ownership Percentage": pct,
            }
