import pandas as pd
from pathlib import Path
from fetools import (
    adjust_fco_table,
    add_accounts_to_fco,
    Account,
    Owner,
    Structure,
    Split,
)


def filter_accounts(
    accounts: pd.DataFrame, clients: pd.DataFrame, fco: pd.DataFrame
) -> pd.DataFrame:
    partially_owned_clients = fco["Owned"].unique().tolist()
    accounts["IsPartiallyOwned"] = accounts["ClientCode"].isin(
        partially_owned_clients
    )
    accounts["IsSMA"] = [bool(acc) for acc in accounts["IsSMA"]]
    filtered_accounts = accounts[
        accounts["IsSMA"] | accounts["IsPartiallyOwned"]
    ]
    final_accounts = adjust_columns(filtered_accounts, clients)
    return final_accounts


def adjust_columns(
    accounts: pd.DataFrame, clients: pd.DataFrame
) -> pd.DataFrame:
    clients = clients[
        ["ClientID", "ClientName", "Jurisdiction"]
    ].drop_duplicates()
    clients = clients.rename(columns={"ClientID": "ClientCode"})
    merged_df = accounts.merge(
        clients,
        how="left",
    )
    cols_to_keep = [
        "AccountCode",
        "AccountName",
        "ClientCode",
        "ClientName",
        "IsSMA",
        "IsPartiallyOwned",
        "AccountCurrencyCode",
        "StartDate",
        "RepCode",
        "AdvisoryScope",
        "AccountUserDefined1",
        "AccountUserDefined2",
        "SMAName",
        "AssetCategory",
        "AssetClass",
        "SubAssetClass",
        "AssetClassLevel3",
    ]
    return merged_df[cols_to_keep]


def create_account_objects(accounts: pd.DataFrame) -> list[Account]:
    account_objects = []
    for _, row in accounts.iterrows():
        account = Account(
            id=row["AccountCode"],
            name=row["AccountName"],
            is_partially_owned=row["IsPartiallyOwned"],
            is_SMA=row["IsSMA"],
            opened_date=row["StartDate"],
            advisory_scope=row["AdvisoryScope"],
            rep_code=row["RepCode"],
            currency=row["AccountCurrencyCode"],
            udf1=row["AccountUserDefined1"],
            udf2=row["AccountUserDefined2"],
            client_id=row["ClientCode"],
            client_name=row["ClientName"],
            sma_name=row["SMAName"],
            sma_category=row["AssetCategory"],
            sma_asset_class=row["AssetClass"],
            sma_asset_class_l2=row["SubAssetClass"],
            sma_asset_class_l3=row["AssetClassLevel3"],
        )
        account_objects.append(account)
    return account_objects


def create_owner_object(owner_df: pd.DataFrame) -> Owner:
    owner_id = owner_df["Owner"].iloc[0]
    owner_name = owner_df["ClientName"].iloc[0]
    ownership = {}
    for _, row in owner_df.iterrows():
        ownership[row["Date"]] = row["Percentage"]

    owner = Owner(id=owner_id, name=owner_name, ownership_structure=ownership)
    return owner


def get_owners(account: Account, fco: pd.DataFrame) -> list[Owner]:
    owners = fco["Owner"].unique().tolist()
    owner_objects = []
    for owner_id in owners:
        owner_df = fco[fco["Owner"] == owner_id]
        owner = create_owner_object(owner_df)
        owner_objects.append(owner)
    return owner_objects


def main(
    accounts_data: pd.DataFrame,
    clients: pd.DataFrame,
    fco: pd.DataFrame,
    cutoff: str,
) -> None:

    adjusted_fco = adjust_fco_table(fco, cutoff)
    accounts_data = filter_accounts(accounts_data, clients, adjusted_fco)
    full_fco = add_accounts_to_fco(adjusted_fco, accounts_data)
    full_fco = full_fco.merge(
        clients[["ClientID", "ClientName"]],
        left_on="Owner",
        right_on="ClientID",
        how="left",
    ).drop(columns=["ClientID"])

    # Gresham specific
    accounts_data["ClientCode"] = accounts_data["ClientCode"].str.lower()
    full_fco["Owner"] = full_fco["Owner"].str.lower()

    # Back to main routine
    household_list = []
    client_list = []
    fund_list = []
    class_series_list = []
    instrument_list = []
    account_create_list = []
    account_remap_list = []
    fund_client_ownership_list = []

    accounts = create_account_objects(accounts_data)
    for account in accounts:
        account_fco = full_fco[full_fco["AccountCode"] == account.id]
        owners = get_owners(account, account_fco)
        structure = Structure(account=account, owners=owners)
        # Further processing can be done with the structure object
        household_list.append(structure.create_household())
        client_list.append(structure.create_client())
        fund_list.append(structure.create_fund())
        class_series_list.append(structure.create_class_series())
        instrument_list.append(structure.create_instrument())
        account_create_list.append(structure.create_account())
        account_remap_list.append(structure.remap_account())
        fund_client_ownership_list.append(structure.fund_client_ownership())

        if account.is_partially_owned:
            for split in structure.create_splits():
                account_create_list.append(split.create_account())
                for fco_entry in split.create_fund_client_ownership():
                    fund_client_ownership_list.append(fco_entry)

    pass


if __name__ == "__main__":
    cutoff = "2020-12-31"
    input_folder = Path("data/inputs/partial_ownership")
    acc = pd.read_csv(Path(input_folder, "Account.csv"))
    cli = pd.read_csv(Path(input_folder, "Client.csv"))
    fco = pd.read_csv(Path(input_folder, "LEOwnership.csv"))
    main(acc, cli, fco, cutoff)
