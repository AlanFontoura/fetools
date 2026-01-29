import pandas as pd
import tomllib
from dataclasses import dataclass

"""
For SMA's:
Account ID
Acount Name
Currency
Client ID
Opened Date
Rep Code
Custodian
Advisory Scope
UDF1
UDF2
UDF5
Asset Classifications
"""


class Structure:
    def __init__(self, df: pd.DataFrame, type="sma"):
        self.df: pd.DataFrame = df
        self.type: str = type.lower()
        self._funds: pd.DataFrame | None = None
        self._classseries: pd.DataFrame | None = None
        self._instruments: pd.DataFrame | None = None
        self._account_create: pd.DataFrame | None = None
        self._account_remap: pd.DataFrame | None = None
        self._fund_client_ownership: pd.DataFrame | None = None

    def create_structure(self):
        pass

    @property
    def funds(self) -> pd.DataFrame:
        if self._funds is None:
            funds = self.df.copy()
            funds["Firm Provided Key"] = [
                f"{self.type}_fund_{account_id}"
                for account_id in funds["Account ID"]
            ]
            funds["Name"] = [
                f"{account_name[:84]} - Fund"
                for account_name in funds["Account Name"]
            ]
            funds["Fund Manager Firm Provided Key"] = funds["Client ID"]
            funds["Type"] = "SMA"
            cols = [
                "Firm Provided Key",
                "Name",
                "Fund Manager Firm Provided Key",
                "Type",
            ]
            funds = funds[cols]
            self._funds = funds
        return self._funds

    @property
    def classseries(self) -> pd.DataFrame:
        if self._classseries is None:
            classseries = self.df.copy()
            classseries["Firm Provided Key"] = [
                f"{self.type}_classseries_{account_id}"
                for account_id in classseries["Account ID"]
            ]
            classseries["Name"] = [
                f"{account_name[:84]} - Class Series"
                for account_name in classseries["Account Name"]
            ]
            classseries["Fund Firm Provided Key"] = [
                f"{self.type}_fund_{account_id}"
                for account_id in classseries["Account ID"]
            ]
            classseries["Weight"] = 1
            cols = [
                "Firm Provided Key",
                "Name",
                "Fund Firm Provided Key",
                "Weight",
            ]
            classseries = classseries[cols]
            self._classseries = classseries
        return self._classseries

    @property
    def instruments(self) -> pd.DataFrame:
        if self._instruments is None:
            instruments = self.df.copy()
            instruments["Instrument ID"] = [
                f"{self.type}_instrument_{account_id}"
                for account_id in instruments["Account ID"]
            ]
            instruments["Firm Security Type Name"] = (
                "SMA" if self.type == "sma" else "Unitless"
            )
            instruments["Currency Name"] = instruments["Currency"]
            instruments["Instrument Name"] = (
                instruments["SMAName"]
                if self.type == "sma"
                else [
                    f"{account_name[:84]} - Instrument"
                    for account_name in instruments["Account Name"]
                ]
            )
            instruments["Class Series ID"] = [
                f"{self.type}_classseries_{account_id}"
                for account_id in instruments["Account ID"]
            ]
            instruments["Valuation Per Position"] = True
            instruments["User Defined 3"] = (
                "SMA" if self.type == "sma" else "Partially Owned"
            )
            cols = [
                "Instrument ID",
                "Instrument Name",
                "Firm Security Type Name",
                "Currency Name",
                "Class Series ID",
                "Valuation Per Position",
                "User Defined 3",
            ]
            if self.type == "sma":
                instruments["Asset Category Name"] = instruments[
                    "AssetCategory"
                ]
                instruments["Asset Class Name"] = instruments["AssetClass"]
                instruments["Asset Class l2 Name"] = instruments[
                    "AssetSubclass"
                ]
                instruments["Asset Class l3 Name"] = instruments[
                    "AssetSubSubclass"
                ]
                instruments["Strategy Name"] = instruments["AssetClass"]
                extra_cols = [
                    "Asset Category Name",
                    "Asset Class Name",
                    "Asset Class l2 Name",
                    "Asset Class l3 Name",
                    "Strategy Name",
                ]
                cols.extend(extra_cols)
            instruments = instruments[cols]
            self._instruments = instruments
        return self._instruments
