import pandas as pd
import numpy as np
from dataclasses import dataclass
from pathlib import Path
import os
import sys
from dataclass_binder import Binder


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
        self._main_fund_client_ownership: pd.DataFrame | None = None

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
        # TODO: Check how to load 'Collapse when scaling' and 'Look through enabled' fields
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
            classseries["Is Look Through Enabled"] = (
                False if self.type == "sma" else True
            )
            classseries["Collapse When Scaling to Client Position"] = (
                True if self.type == "sma" else False
            )
            cols = [
                "Firm Provided Key",
                "Name",
                "Fund Firm Provided Key",
                "Weight",
                "Is Look Through Enabled",
                "Collapse When Scaling to Client Position",
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
                (
                    f"sma_account_{account_id}"
                    if self.type == "sma"
                    else f"po_direct_{account_id}"
                )
                for account_id in account_create["Account ID"]
            ]
            account_create["Account Name"] = (
                [
                    f"{account_name[:94]} - SMA"
                    for account_name in account_create["Account Name"]
                ]
                if self.type == "sma"
                else [
                    f"{account_name[:79]} - PO Direct Account"
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
                "PO - Direct Account" if self.type == "po" else None
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
    def main_fund_client_ownership(self) -> pd.DataFrame | None:
        if self._main_fund_client_ownership is None:
            fund_client_ownership = self.df.copy()
            fund_client_ownership["Class Series ID"] = [
                f"{self.type}_classseries_{account_id}"
                for account_id in fund_client_ownership["Account ID"]
            ]
            fund_client_ownership["Client Account ID"] = [
                (
                    f"sma_account_{account_id}"
                    if self.type == "sma"
                    else f"po_direct_{account_id}"
                )
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
            self._main_fund_client_ownership = fund_client_ownership
        return self._main_fund_client_ownership

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
        self._main_fund_client_ownership = pd.concat(
            [
                self.main_fund_client_ownership,
                other.main_fund_client_ownership,
            ],
            ignore_index=True,
        )
        self.type = "both"
        return self

    def write_to_folder(self, folder_path: str):
        os.makedirs(os.path.join(folder_path, "funds"), exist_ok=True)
        os.makedirs(os.path.join(folder_path, "classseries"), exist_ok=True)
        os.makedirs(os.path.join(folder_path, "importers"), exist_ok=True)
        self.funds.to_csv(f"{folder_path}/funds/funds.csv", index=False)
        self.classseries.to_csv(
            f"{folder_path}/classseries/classseries.csv", index=False
        )
        self.instruments.to_csv(
            f"{folder_path}/importers/Instruments.csv", index=False
        )
        self.account_create.to_csv(
            f"{folder_path}/importers/MainAccountCreate.csv", index=False
        )
        self.account_remap.to_csv(
            f"{folder_path}/importers/AccountRemap.csv", index=False
        )
        if self.main_fund_client_ownership is not None:
            self.main_fund_client_ownership.to_csv(
                f"{folder_path}/importers/MainFundClientOwnership.csv",
                index=False,
            )


def create_structure_files(config: PO_SMA_Config) -> Structure:
    df = pd.read_csv(config.account_file)
    # TODO: Add logic to filter account file based on ownership data if needed
    if config.type == "sma":
        structure = Structure(df, type="sma")

    if config.type == "po":
        structure = Structure(df, type="po")

    if config.type == "both":
        sma_df = df[df["Is SMA"]]
        po_df = df[~df["Is SMA"]]
        sma_structure = Structure(sma_df, type="sma")
        po_structure = Structure(po_df, type="po")
        structure = sma_structure.merge(po_structure)

    return structure


# endregion


# region Ownership file related functions
def validate_ownership_file(df: pd.DataFrame) -> pd.DataFrame:
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
            "Invalid percentage values found in ownership file."
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
        # raise ValueError(
        #     "Some entities are over 100% owned on certain dates."
        # )
        print("Warning: Some entities are over 100% owned on certain dates.")
    under_owned = total_ownership[total_ownership["Percentage"] < 0.98]
    if not under_owned.empty:
        # raise ValueError(
        #     "Some entities are under 100% owned on certain dates."
        # )
        print("Warning: Some entities are under 100% owned on certain dates.")
    return df


def add_zero_entries(df: pd.DataFrame) -> pd.DataFrame:
    new_rows = []
    for owned_entity, group in df.groupby("Owned"):
        dates = sorted(group["Date"].unique())

        for i in range(1, len(dates)):
            prev_date, curr_date = dates[i - 1], dates[i]

            prev_owners = set(group.loc[group["Date"] == prev_date, "Owner"])
            curr_owners = set(group.loc[group["Date"] == curr_date, "Owner"])

            disappeared = prev_owners - curr_owners

            for owner in disappeared:
                new_rows.append(
                    {
                        "Owner": owner,
                        "Owned": owned_entity,
                        "Date": curr_date,
                        "Percentage": 0.0,
                    }
                )
    zero_entries_df = pd.DataFrame(new_rows)
    new_df = pd.concat([df, zero_entries_df], ignore_index=True)
    new_df = new_df.sort_values(by=["Date", "Owner", "Owned"]).reset_index(
        drop=True
    )
    return new_df


def extend_ownership_table(df: pd.DataFrame) -> pd.DataFrame:
    all_dates = sorted(df["Date"].unique())
    extended_ownership: list[pd.DataFrame] = []
    for date in all_dates:
        current_snapshot = get_current_snapshot(df, date)
        ooo_snapshot = multi_level_ooo(current_snapshot)
        extended_ownership.append(ooo_snapshot)
    extend_ownership_table = (
        pd.concat(extended_ownership, ignore_index=True)
        .drop_duplicates()
        .sort_values(by=["Date", "Owned", "Owner"])
        .reset_index(drop=True)
    )
    return extend_ownership_table


def get_current_snapshot(df: pd.DataFrame, as_of_date: str) -> pd.DataFrame:
    snapshot = (
        df[df["Date"] <= as_of_date]
        .sort_values(by=["Date", "Owned", "Owner"])
        .drop_duplicates(subset=["Owner", "Owned"], keep="last")
        .reset_index(drop=True)
    )
    return snapshot


def multi_level_ooo(df: pd.DataFrame) -> pd.DataFrame:
    total_entries = df.shape[0]
    keep_going = True

    while keep_going:
        df = single_level_ooo(df)
        new_total_entries = df.shape[0]
        if new_total_entries == total_entries:
            keep_going = False
        else:
            total_entries = new_total_entries
    df = aggregate_ownership(df)
    return df


def single_level_ooo(df: pd.DataFrame) -> pd.DataFrame:
    ooo = df.merge(
        df,
        left_on="Owned",
        right_on="Owner",
        how="inner",
    )
    ooo["Date"] = ooo[["Date_x", "Date_y"]].max(axis=1)
    ooo["Percentage"] = ooo["Percentage_x"] * ooo["Percentage_y"]
    ooo = ooo[["Owner_x", "Owned_y", "Date", "Percentage"]]
    ooo = ooo.rename(columns={"Owner_x": "Owner", "Owned_y": "Owned"})
    extended_df = (
        pd.concat([df, ooo])
        .drop_duplicates()
        .sort_values(by=["Date", "Owned", "Owner"])
        .reset_index(drop=True)
    )
    return extended_df


def aggregate_ownership(df: pd.DataFrame) -> pd.DataFrame:
    aggregate_logic = {"Date": "max", "Percentage": "sum"}
    aggregated = df.groupby(["Owner", "Owned"], as_index=False).agg(
        aggregate_logic
    )
    return aggregated


def get_ownership_file(file_path: str | None) -> pd.DataFrame:
    if file_path is None:
        return pd.DataFrame()
    df = pd.read_csv(file_path)
    df = validate_ownership_file(df)
    df = add_zero_entries(df)
    df = extend_ownership_table(df)
    return df


def filter_ownership_by_date(
    df: pd.DataFrame, cutoff_date: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    prior = df[df["Date"] <= cutoff_date]
    after = df[df["Date"] > cutoff_date]
    prior = prior.sort_values(by=["Owned", "Date", "Owner"]).drop_duplicates(
        subset=["Owned", "Owner"], keep="last"
    )
    prior["Date"] = cutoff_date
    current_ownership = pd.concat([prior, after], ignore_index=True)
    past_ownership = df[df["Date"] < cutoff_date]
    return current_ownership, past_ownership


# endregion


# region Split Accounts and FCO Loader functions
def create_split_accounts_file(
    account: pd.DataFrame, ownership: pd.DataFrame
) -> pd.DataFrame:
    ownership = ownership.sort_values(
        by=["Owned", "Date", "Owner"]
    ).reset_index(drop=True)
    ownership_dates = ownership.drop_duplicates(
        subset=["Owner", "Owned"], keep="first"
    ).drop(columns=["Percentage"])
    ownership_pcts = ownership.drop_duplicates(
        subset=["Owner", "Owned"], keep="last"
    ).drop(columns=["Date"])
    ownership = ownership_dates.merge(
        ownership_pcts,
        on=["Owner", "Owned"],
        how="inner",
    )
    full_data = ownership.merge(
        account,
        left_on="Owned",
        right_on="Client ID",
        how="inner",
    )
    full_data["Date"] = pd.to_datetime(full_data["Date"])
    full_data["Opened Date"] = pd.to_datetime(full_data["Opened Date"])
    full_data["Account Type Name"] = "Other"
    full_data["Account ID"] = [
        f"po_split_{account_id}_{owner}"
        for owner, account_id in zip(
            full_data["Owner"], full_data["Account ID"]
        )
    ]
    full_data["Account Name"] = [
        f"{account_name[:90]} - {100*pct:.2f}%"
        for account_name, pct in zip(
            full_data["Account Name"], full_data["Percentage"]
        )
    ]
    full_data["Currency Name"] = full_data["Currency"]
    full_data["Client ID"] = full_data["Owner"]
    full_data["Date Opened"] = [
        max(open_date, ownership_date)
        for open_date, ownership_date in zip(
            full_data["Opened Date"], full_data["Date"]
        )
    ]
    full_data["Inception Date"] = full_data["Date Opened"]
    full_data["Rep Code ID"] = full_data["Rep Code"]
    full_data["Custodian Name"] = full_data["Custodian"]
    full_data["Advisory Scope Name"] = full_data["Advisory Scope"]
    full_data["User Defined 1"] = full_data["UDF1"]
    full_data["User Defined 2"] = full_data["UDF2"]
    full_data["User Defined 5"] = "PO - Split Account"

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
    full_data = full_data[cols]

    return full_data


def create_fco_loader(
    accounts: pd.DataFrame, ownership: pd.DataFrame
) -> pd.DataFrame:
    fco_table = ownership.merge(
        accounts,
        left_on="Owned",
        right_on="Client ID",
        how="inner",
    )
    fco_table["Class Series ID"] = [
        (
            f"sma_classseries_{account_id}"
            if is_sma
            else f"po_classseries_{account_id}"
        )
        for account_id, is_sma in zip(
            fco_table["Account ID"], fco_table["Is SMA"]
        )
    ]
    fco_table["Client Account ID"] = [
        f"po_split_{account_id}_{owner}"
        for owner, account_id in zip(
            fco_table["Owner"], fco_table["Account ID"]
        )
    ]
    fco_table["Date"] = fco_table["Date"]
    fco_table["Percent"] = fco_table["Percentage"]
    cols = [
        "Class Series ID",
        "Client Account ID",
        "Date",
        "Percent",
    ]
    fco_table = fco_table[cols]
    return fco_table


# endregion


# region Main Partial Ownership function
def create_partial_ownership_loaders(
    config: PO_SMA_Config,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    accounts = pd.read_csv(config.account_file)
    fco_table = get_ownership_file(config.ownership_file)

    splits = create_split_accounts_file(
        accounts,
        fco_table,
    )

    if config.first_transaction_date is not None:
        fco_table, _ = filter_ownership_by_date(
            fco_table,
            config.first_transaction_date,
        )

    fco_loader = create_fco_loader(accounts, fco_table)

    return splits, fco_loader


# endregion


# region Main script
def main():
    if len(sys.argv) < 2:
        print("Usage: po-sma <config_file.toml>")
        sys.exit(1)

    config_file_path = sys.argv[1]
    # Read config file
    config_file = Path(config_file_path)
    config = Binder(PO_SMA_Config).parse_toml(config_file)

    # Create main structure files
    structure = create_structure_files(config)
    structure.write_to_folder(config.output_folder)

    # Create Partial Ownership files
    if config.type in ("po", "both"):
        split_account_loader, fco_loader = create_partial_ownership_loaders(
            config
        )
        split_account_loader.to_csv(
            Path(config.output_folder)
            / "importers"
            / "SplitAccountCreate.csv",
            index=False,
        )
        fco_loader.to_csv(
            Path(config.output_folder)
            / "importers"
            / "SplitFundClientOwnership.csv",
            index=False,
        )


if __name__ == "__main__":
    main()
# endregion
