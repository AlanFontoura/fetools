import pandas as pd
import numpy as np
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
        raise ValueError(
            "Some entities are over 100% owned on certain dates."
        )
    under_owned = total_ownership[total_ownership["Percentage"] < 0.98]
    if not under_owned.empty:
        raise ValueError(
            "Some entities are under 100% owned on certain dates."
        )
    return df


def add_zero_entries(df: pd.DataFrame) -> pd.DataFrame:
    # Ensure dates are datetime objects
    df["Date"] = pd.to_datetime(df["Date"])

    new_rows = []
    # Process each 'Owned' entity individually
    for owned_entity, group in df.groupby("Owned"):
        # Get unique dates for this specific entity in order
        dates = sorted(group["Date"].unique())

        for i in range(1, len(dates)):
            prev_date = dates[i - 1]
            curr_date = dates[i]

            # Owners present in the previous snapshot
            prev_owners = set(group[group["Date"] == prev_date]["Owner"])
            # Owners present in the current snapshot
            curr_owners = set(group[group["Date"] == curr_date]["Owner"])

            # Find owners who "disappeared"
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
    new_df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
    new_df = new_df.sort_values(by=["Owned", "Date", "Owner"]).reset_index(
        drop=True
    )
    return new_df


def resolve_effective_ownership(df: pd.DataFrame) -> pd.DataFrame:
    timeline = sorted(df["Date"].unique())
    all_snapshots = []
    last_snapshot = pd.DataFrame()  # To keep track of the previous state

    for current_date in timeline:
        as_of_now = df[df["Date"] <= current_date]

        # 1. Get current direct state (Overwrite logic)
        current_state = as_of_now.drop_duplicates(
            subset=["Owner", "Owned"], keep="last"
        )
        current_state = current_state[current_state["Percentage"] > 1e-6]

        if current_state.empty:
            continue

        # 2. Expand to effective ownership (Add logic)
        this_snapshot = _calculate_full_path_expansion(current_state)
        this_snapshot["Date"] = current_date

        # 3. THE FIX: Change Detection
        # If this is not the first date, only keep rows that are NEW or CHANGED
        if not last_snapshot.empty:
            # Merge current with previous to compare percentages
            comparison = this_snapshot.merge(
                last_snapshot[["Owner", "Owned", "Percentage"]],
                on=["Owner", "Owned"],
                how="left",
                suffixes=("", "_prev"),
            )
            # Filter for rows where percentage changed or the link is brand new
            # We use np.isclose to handle tiny floating point math differences
            changed_mask = ~np.isclose(
                comparison["Percentage"],
                comparison["Percentage_prev"].fillna(-1),
            )
            this_snapshot = this_snapshot[changed_mask].copy()

        # 4. Update the 'last_snapshot' with the FULL state (before filtering)
        # We need the full state for the next date's comparison
        # (But we only add the 'changes' to our final report)
        full_current_resolved = _calculate_full_path_expansion(current_state)
        last_snapshot = full_current_resolved

        if not this_snapshot.empty:
            all_snapshots.append(this_snapshot)

    return pd.concat(all_snapshots, ignore_index=True)


def _calculate_full_path_expansion(state_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates both direct and indirect ownership, preserving all
    intermediate links (e.g., A -> K and X -> K).
    """
    nodes = sorted(list(set(state_df["Owner"]).union(set(state_df["Owned"]))))
    node_map = {node: i for i, node in enumerate(nodes)}
    n = len(nodes)

    # M1 = Direct ownership matrix
    M1 = np.zeros((n, n))
    for _, row in state_df.iterrows():
        M1[node_map[row["Owner"]], node_map[row["Owned"]]] = row["Percentage"]

    # We will accumulate all levels of ownership here
    # Start with Direct (Level 1)
    full_results_matrix = M1.copy()

    # Now calculate Indirect (Level 2, 3, etc.)
    # M_current represents the 'flow' at the next depth
    M_current = M1.copy()

    # We loop up to the number of entities to ensure we catch deep chains
    for _ in range(n):
        # Matrix multiplication finds the next level of indirect ownership
        # M_next = (Owners of Middlemen) * (Middlemen's ownership of targets)
        M_next = M_current @ M1

        if np.all(M_next < 1e-9):  # Stop if no more indirect links are found
            break

        # IMPORTANT: We ADD the indirect interest to our total matrix
        full_results_matrix += M_next
        M_current = M_next

    # Convert back to DataFrame
    rows, cols = np.where(full_results_matrix > 1e-7)
    results = []
    for r, c in zip(rows, cols):
        results.append(
            {
                "Owner": nodes[r],
                "Owned": nodes[c],
                "Percentage": round(full_results_matrix[r, c], 6),
            }
        )

    return pd.DataFrame(results)


def get_ownership_file(file_path: str | None) -> pd.DataFrame:
    if file_path is None:
        return pd.DataFrame()
    df = pd.read_csv(file_path)
    df = validate_ownership_file(df)
    df = add_zero_entries(df)
    df = resolve_effective_ownership(df)
    # extended_df = extend_ownership(df)
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
