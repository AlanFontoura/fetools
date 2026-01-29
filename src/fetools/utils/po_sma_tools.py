import pandas as pd
from dataclasses import dataclass

"""
Account input file (fields with a * are optional):
Account ID
Account Name
Currency
Client ID
Opened Date
Rep Code
Custodian
Advisory Scope
UDF1
UDF2
UDF5
SMA Name (SMA only)
*Asset Category (SMA only)
*Asset Class (SMA only)
*Sub Asset Class (SMA only)
*Asset Class Level3 (SMA only)
*Asset Strategy (SMA only)

Ownership input file (for PO only):
Owner           Owner Client ID
Owned           Owned Entity Client ID
Date            Date of Ownership
Percentage      Percentage of Ownership
"""


# region Config dataclass
@dataclass(frozen=True)
class PO_SMA_Config:
    type: str  # "po", "sma", or "both"
    first_transaction_date: str
    output_folder: str
    account_file: str
    ownership_file: str | None = None


# endregion


# region Structure class and related functions
class Structure:
    def __init__(self, df: pd.DataFrame, type="sma"):
        self.df: pd.DataFrame = df
        self.type: str = type.lower()
        if self.type not in ["sma", "po"]:
            raise ValueError(
                f"Invalid type: '{self.type}'. Type must be either 'SMA' or 'PO'."
            )
        self._funds: pd.DataFrame | None = None
        self._classseries: pd.DataFrame | None = None
        self._instruments: pd.DataFrame | None = None
        self._account_create: pd.DataFrame | None = None
        self._account_remap: pd.DataFrame | None = None
        self._fund_client_ownership: pd.DataFrame | None = None

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
                instruments["SMA Name"]
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
                    "Asset Category"
                ]
                instruments["Asset Class Name"] = instruments["Asset Class"]
                instruments["Asset Class l2 Name"] = instruments[
                    "Sub Asset Class"
                ]
                instruments["Asset Class l3 Name"] = instruments[
                    "Asset Class Level3"
                ]
                instruments["Strategy Name"] = instruments["Asset Strategy"]
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

    @property
    def account_create(self) -> pd.DataFrame:
        if self._account_create is None:
            account_create = self.df.copy()
            account_create["Account Type Name"] = "Other"
            account_create["Account ID"] = [
                f"{self.type}_account_{account_id}"
                for account_id in account_create["Account ID"]
            ]
            account_create["Account Name"] = (
                [
                    f"{account_name[:84]} - SMA"
                    for account_name in account_create["Account Name"]
                ]
                if self.type == "sma"
                else [
                    f"{account_name[:84]} - PO Account"
                    for account_name in account_create["Account Name"]
                ]
            )
            account_create["Currency Name"] = account_create["Currency"]
            account_create["Client ID"] = account_create["Client ID"]
            account_create["Date Opened"] = account_create["Opened Date"]
            account_create["Inception Date"] = account_create["Opened Date"]
            account_create["Rep Code ID"] = account_create["Rep Code"]
            account_create["Custodian Name"] = account_create["Custodian"]
            account_create["Advisory Scope Name"] = account_create[
                "Advisory Scope"
            ]
            account_create["User Defined 1"] = account_create["UDF1"]
            account_create["User Defined 2"] = account_create["UDF2"]
            account_create["User Defined 5"] = (
                "Partially Owned" if self.type == "po" else None
            )
            cols = [
                "Account Type Name",
                "Account ID",
                "Account Name",
                "Currency Name",
                "Client ID",
                "Date Opened",
                "Inception Date",
                "Rep Code ID",
                "Custodian Name",
                "Advisory Scope Name",
                "User Defined 1",
                "User Defined 2",
                "User Defined 5",
            ]
            account_create = account_create[cols]
            self._account_create = account_create
        return self._account_create

    @property
    def account_remap(self) -> pd.DataFrame:
        if self._account_remap is None:
            account_remap = self.df.copy()
            account_remap["Class Series ID"] = [
                f"{self.type}_classseries_{account_id}"
                for account_id in account_remap["Account ID"]
            ]
            account_remap["Client ID"] = None
            cols = ["Account ID", "Class Series ID", "Client ID"]
            account_remap = account_remap[cols]
            self._account_remap = account_remap
        return self._account_remap

    @property
    def fund_client_ownership(self) -> pd.DataFrame:
        if self._fund_client_ownership is None:
            fund_client_ownership = self.df.copy()
            fund_client_ownership["Class Series ID"] = [
                f"{self.type}_classseries_{account_id}"
                for account_id in fund_client_ownership["Account ID"]
            ]
            fund_client_ownership["Client Account ID"] = [
                f"{self.type}_account_{account_id}"
                for account_id in fund_client_ownership["Account ID"]
            ]
            fund_client_ownership["Date"] = fund_client_ownership[
                "Opened Date"
            ]
            fund_client_ownership["Percent"] = 1
            cols = [
                "Class Series ID",
                "Client Account ID",
                "Date",
                "Percent",
            ]
            fund_client_ownership = fund_client_ownership[cols]
            self._fund_client_ownership = fund_client_ownership
        return self._fund_client_ownership

    def merge(self, other):
        if self.type == other.type:
            raise ValueError(
                "Cannot merge two Structure objects of the same type."
            )
        merged_df = pd.concat([self.df, other.df], ignore_index=True)
        self._funds = pd.concat([self.funds, other.funds], ignore_index=True)
        self._classseries = pd.concat(
            [self.classseries, other.classseries], ignore_index=True
        )
        self._instruments = pd.concat(
            [self.instruments, other.instruments], ignore_index=True
        )
        self._account_create = pd.concat(
            [self.account_create, other.account_create], ignore_index=True
        )
        self._account_remap = pd.concat(
            [self.account_remap, other.account_remap], ignore_index=True
        )
        self._fund_client_ownership = pd.concat(
            [self.fund_client_ownership, other.fund_client_ownership],
            ignore_index=True,
        )
        self.type = "both"
        return self


def create_structure_files(file_path: str, type: str) -> Structure:
    df = pd.read_csv(file_path)
    if type == "sma":
        structure = Structure(df, type="sma")

    if type == "po":
        structure = Structure(df, type="po")

    if type == "both":
        sma_df = df[df["Is SMA"]]
        po_df = df[~df["Is SMA"]]
        sma_structure = Structure(sma_df, type="sma")
        po_structure = Structure(po_df, type="po")
        structure = sma_structure.merge(po_structure)

    return structure


# endregion


# region Ownership file related functions
def validate_ownership_file(df: pd.DataFrame) -> bool:
    required_columns = [
        "Owner",
        "Owned",
        "Date",
        "Percentage",
    ]
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")
    df = df.groupby(["Owner", "Owned", "Date"]).sum().reset_index()
    invalid_entries = df[(df["Percentage"] > 1) | (df["Percentage"] < 0)]
    if not invalid_entries.empty:
        raise ValueError(
            "Invalid percentage values found in ownership file. "
            "Percentages must be between 0 and 1."
        )
    self_ownership = df[df["Owner"] == df["Owned"]]
    if not self_ownership.empty:
        raise ValueError("Self-ownership entries found in ownership file.")
    total_ownership = (
        df.groupby(["Owned", "Date"])["Percentage"].sum().reset_index()
    )
    over_owned = total_ownership[total_ownership["Percentage"] > 1.02]
    if not over_owned.empty:
        raise ValueError(
            "Some entities are over 100% owned on certain dates."
        )
    under_owned = total_ownership[total_ownership["Percentage"] < 0.98]
    if not under_owned.empty:
        raise ValueError(
            "Some entities are under 100% owned on certain dates."
        )
    return True


def include_ownership_of_ownership(df: pd.DataFrame) -> pd.DataFrame:
    ownership = df.merge(
        df,
        left_on="Owned",
        right_on="Owner",
        how="inner",
        suffixes=("_1", "_2"),
    )
    ownership["Date"] = ownership[["Date_1", "Date_2"]].max(axis=1)
    ownership["Percentage"] = (
        ownership["Percentage_1"] * ownership["Percentage_2"]
    )
    ownership = ownership[["Owner_1", "Owned_2", "Date", "Percentage"]]
    ownership = ownership.rename(
        columns={"Owner_1": "Owner", "Owned_2": "Owned"}
    )
    return ownership


def extend_ownership(df: pd.DataFrame) -> pd.DataFrame:
    total_entries = df.shape[0]
    while True:
        new_ownership = include_ownership_of_ownership(df)
        df = pd.concat([df, new_ownership], ignore_index=True)
        df = df.drop_duplicates()
        if df.shape[0] == total_entries:
            break
        total_entries = df.shape[0]
    df["Percentage"] = df["Percentage"].round(4)
    return df


def get_ownership_file(file_path: str | None) -> pd.DataFrame:
    if file_path is None:
        return pd.DataFrame()
    df = pd.read_csv(file_path)
    validate_ownership_file(df)
    extended_df = extend_ownership(df)
    extended_df = extended_df.sort_values(
        by=["Owned", "Owner", "Date"]
    ).reset_index(drop=True)
    return extended_df


def filter_ownership_by_date(
    df: pd.DataFrame, cutoff_date: str
) -> pd.DataFrame:
    prior = df[df["Date"] <= cutoff_date]
    after = df[df["Date"] > cutoff_date]
    prior = prior.sort_values(by=["Owned", "Owner", "Date"]).drop_duplicates(
        subset=["Owned", "Owner"], keep="last"
    )
    prior["Date"] = cutoff_date
    current_ownership = pd.concat([prior, after], ignore_index=True)
    past_ownership = df[df["Date"] < cutoff_date]
    return current_ownership, past_ownership


# endregion


def create_split_accounts_file(
    account: pd.DataFrame, ownership: pd.DataFrame
) -> pd.DataFrame:
    full_data = ownership.merge(
        account,
        left_on="Owned",
        right_on="Client ID",
        how="inner",
    )
    return full_data
