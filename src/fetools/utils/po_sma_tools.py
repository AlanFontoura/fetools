import pandas as pd
from dataclasses import dataclass
from typing import cast


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

    def create_splits(self):
        for owner in self.owners:
            split = Split(account=self.account, owner=owner)
            yield split


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


def extend_ownership_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extends the ownership DataFrame by adding multi level ownership of ownership
    entries.

    Parameters:
    df (pd.DataFrame): DataFrame containing 'Owner', 'Owned', 'Date' and 'Percentage'
    columns.

    Returns:
    pd.DataFrame: Extended DataFrame with additional multi level ownership of
    ownership entries.
    """
    number_of_entries = df.shape[0]
    keep_going = True
    while keep_going:
        new_entries = simple_ownership_of_ownership(df)
        df = pd.concat([df, new_entries], ignore_index=True).drop_duplicates()
        if df.shape[0] == number_of_entries:
            keep_going = False
        else:
            number_of_entries = df.shape[0]
    df["Percentage"] = df["Percentage"].round(4)
    return df.sort_values(by=["Owned", "Owner", "Date"]).reset_index(
        drop=True
    )


def simple_ownership_of_ownership(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates a single level of ownership of ownership for each entity in the
    DataFrame.

    Parameters:
    df (pd.DataFrame): DataFrame containing 'Owner', 'Owned', 'Date' and 'Percentage'
    columns.

    Returns:
    pd.DataFrame: DataFrame with additional entries representing one level of
    ownership of ownership.
    """
    ownership_of_ownership = df.merge(
        df, left_on="Owned", right_on="Owner", how="inner"
    )
    ownership_of_ownership["Date"] = [
        max(d1, d2)
        for d1, d2 in zip(
            ownership_of_ownership["Date_x"], ownership_of_ownership["Date_y"]
        )
    ]
    ownership_of_ownership = ownership_of_ownership.sort_values(
        by=["Owned_y", "Owner_x", "Date", "Date_x", "Date_y"]
    ).drop_duplicates(subset=["Owner_x", "Owned_y", "Date"], keep="last")
    ownership_of_ownership["Owner"] = ownership_of_ownership["Owner_x"]
    ownership_of_ownership["Owned"] = ownership_of_ownership["Owned_y"]
    ownership_of_ownership["Percentage"] = (
        ownership_of_ownership["Percentage_x"]
        * ownership_of_ownership["Percentage_y"]
    )
    ownership_of_ownership = ownership_of_ownership[
        ["Owner", "Owned", "Date", "Percentage"]
    ]
    return ownership_of_ownership


def apply_cutoff_date_to_fco(
    df: pd.DataFrame, cutoff_date: str
) -> pd.DataFrame:
    """
    Applies a cutoff date to the ownership DataFrame
    On the cutoff date, only the latest entry prior to or on the cutoff date
    is kept for each Owned-Owner pair. Entries after the cutoff date are kept as is

    Parameters:
    df (pd.DataFrame): DataFrame containing 'Date' column.
    cutoff_date (str format, YYYY-MM-DD): The cutoff date to filter the DataFrame.

    Returns:
    pd.DataFrame: Filtered DataFrame with entries on or after the cutoff date.
    """
    df = df.sort_values(by=["Owned", "Date", "Owner"]).reset_index(drop=True)
    after = df[df["Date"] > cutoff_date]
    prior = df[df["Date"] <= cutoff_date]
    if not prior.empty:
        prior = prior.drop_duplicates(subset=["Owned", "Owner"], keep="last")
        prior["Date"] = cutoff_date
    df = pd.concat([prior, after], ignore_index=True)
    return df.sort_values(by=["Owned", "Date", "Owner"]).reset_index(
        drop=True
    )


def add_zero_entries_to_owner(
    fco: pd.DataFrame = pd.DataFrame(),
) -> pd.DataFrame:
    """
    Adds zero percentage entries to the ownership DataFrame
    for all combinations of Owners and Owned entities that do not exist in the DataFrame.

    Parameters:
    fco (pd.DataFrame): DataFrame containing 'Owner', 'Owned', 'Date' and 'Percentage'
    columns for a single 'Owned' entity.

    Returns:
    pd.DataFrame: DataFrame with added zero percentage entries.
    """
    if fco.empty:
        return fco
    dates = fco["Date"].unique().tolist()
    dates.sort()
    owned = fco["Owned"].iloc[0]
    current_owners = (
        fco.loc[fco["Date"] == dates[0], "Owner"].unique().tolist()
    )
    for date in dates[1:]:
        owners_at_date = (
            fco.loc[fco["Date"] == date, "Owner"].unique().tolist()
        )
        for owner in current_owners:
            if owner not in owners_at_date:
                new_entry = pd.DataFrame(
                    {
                        "Owner": [owner],
                        "Owned": [owned],
                        "Date": [date],
                        "Percentage": [0.0],
                    }
                )
                fco = cast(
                    pd.DataFrame,
                    pd.concat([fco, new_entry], ignore_index=True),
                )
        current_owners = owners_at_date
    return fco.sort_values(by=["Date", "Owner"]).reset_index(drop=True)


def add_zero_entries_to_fco(
    df: pd.DataFrame = pd.DataFrame(),
) -> pd.DataFrame:
    """
    Adds zero percentage entries to the ownership DataFrame
    for all combinations of Owners and Owned entities that do not exist in the DataFrame.

    Parameters:
    df (pd.DataFrame): DataFrame containing 'Owner', 'Owned', 'Date' and 'Percentage'
    columns.

    Returns:
    pd.DataFrame: DataFrame with added zero percentage entries.
    """
    if df.empty:
        return df
    fco_groups = []
    owned_entities = df["Owned"].unique().tolist()
    for owned in owned_entities:
        fco_owned = df[df["Owned"] == owned].reset_index(drop=True)
        fco_owned_extended = add_zero_entries_to_owner(fco_owned)
        fco_groups.append(fco_owned_extended)
    return (
        pd.concat(fco_groups, ignore_index=True)
        .sort_values(by=["Owned", "Date", "Owner"])
        .reset_index(drop=True)
    )


# TODO: implement FCO input validation
def validate_fco(df: pd.DataFrame) -> None:
    pass


def adjust_fco_table(
    df: pd.DataFrame,
    cutoff_date: str,
) -> pd.DataFrame:
    """
    Adjusts the ownership DataFrame by applying a cutoff date and adding zero
    percentage entries.

    Parameters:
    df (pd.DataFrame): DataFrame containing 'Owner', 'Owned', 'Date' and 'Percentage'
    columns.
    cutoff_date (str format, YYYY-MM-DD): The cutoff date to filter the DataFrame.

    Returns:
    pd.DataFrame: Adjusted DataFrame with applied cutoff date and added zero
    percentage entries.
    """
    df = extend_ownership_table(df)
    df = apply_cutoff_date_to_fco(df, cutoff_date)
    df = add_zero_entries_to_fco(df)
    return df.sort_values(by=["Owned", "Date", "Owner"]).reset_index(
        drop=True
    )


def add_accounts_to_fco(
    fco: pd.DataFrame,
    acc: pd.DataFrame,
) -> pd.DataFrame:
    account_fco = fco.merge(
        acc[["ClientCode", "AccountCode"]],
        left_on="Owned",
        right_on="ClientCode",
        how="left",
    )
    account_fco = account_fco.drop(columns=["ClientCode", "Owned"])
    return account_fco.sort_values(
        by=["AccountCode", "Date", "Owner"]
    ).reset_index(drop=True)
