import pandas as pd
from dataclass_binder import Binder
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from fetools.utils.exceptions import MissingConfigError


# region Dataclasses for config
@dataclass(frozen=True)
class SMAConfig:
    recon: bool = False
    prefix: str = ""
    threshold: float = 0.0
    detailed: bool = False


@dataclass(frozen=True)
class PartialOwnershipConfig:
    recon: bool = False
    prefix_direct: str = ""
    prefix_split: str = ""
    threshold: float = 0.0
    detailed: bool = False


@dataclass(frozen=True)
class Config:
    client: str
    env: str
    region: str
    sma: SMAConfig = SMAConfig()
    partial_ownership: PartialOwnershipConfig = PartialOwnershipConfig()


# endregion


# region Get and clean data
def get_tracking_data(config: Config) -> pd.DataFrame:
    tracking_file_path = f"s3://d1g1t-client-{config.region}/{config.client}/exports/{config.env}-{config.client}-monthly-tracking.csv"
    tracking_chunks = []
    processed_rows = 0
    for chunk in pd.read_csv(tracking_file_path, chunksize=100_000):
        tracking_chunks.append(chunk)
        processed_rows += len(chunk)
        print(f"Processed rows: {processed_rows:,}")
    tracking_data = pd.concat(tracking_chunks, ignore_index=True)
    tracking_data = clean_tracking_data(tracking_data)
    return tracking_data


def clean_tracking_data(tracking_data: pd.DataFrame) -> pd.DataFrame:
    tracking_data = tracking_data[tracking_data["is_dead"] == "f"]
    tracking_data = tracking_data.drop(
        columns=["is_dead", "ai", "start_date", "scale", "price"]
    )
    tracking_data["mv"] = tracking_data["mv"].round(2)
    tracking_data = tracking_data.sort_values(
        by=["account", "date", "instrument"]
    ).reset_index(drop=True)
    return tracking_data


# endregion


# region SMA recon
def sma_recon(df: pd.DataFrame, sma_config: SMAConfig) -> pd.DataFrame:
    sma_accounts = (
        df.loc[df["account"].str.contains(sma_config.prefix), "account"]
        .drop_duplicates()
        .tolist()
    )
    original_accounts = [
        account.replace(sma_config.prefix, "") for account in sma_accounts
    ]

    sma_data = (
        df[df["account"].isin(sma_accounts)]
        .drop(columns=["units", "instrument"])
        .rename(columns={"mv": "sma_mv", "account": "sma_account"})
    )
    sma_data["account"] = sma_data["sma_account"].str.replace(
        sma_config.prefix, "", regex=False
    )
    original_data = df[df["account"].isin(original_accounts)]
    aggregated_original_data = (
        original_data.groupby(["date", "account"], as_index=False)
        .agg({"mv": "sum"})
        .rename(columns={"mv": "original_mv"})
    )
    recon = pd.merge(
        sma_data,
        aggregated_original_data,
        on=["date", "account"],
        how="outer",
    ).fillna(0)
    recon["mv_diff"] = (recon["sma_mv"] - recon["original_mv"]).abs()
    recon = recon[recon["mv_diff"] > sma_config.threshold].reset_index(
        drop=True
    )
    cols = [
        "date",
        "account",
        "sma_account",
        "original_mv",
        "sma_mv",
        "mv_diff",
    ]
    recon = recon[cols]

    if sma_config.detailed:
        recon = recon.merge(df, on=["date", "account"], how="left")

        cols = [
            "date",
            "account",
            "sma_account",
            "instrument",
            "units",
            "mv",
            "original_mv",
            "sma_mv",
            "mv_diff",
        ]

        recon = recon[cols]

    recon = recon.sort_values(by=["account", "date"]).reset_index(drop=True)
    return recon


# endregion


# region Partial ownership recon
def partial_ownership_recon(
    df: pd.DataFrame, po_config: PartialOwnershipConfig
) -> pd.DataFrame:
    direct_recon = po_direct_recon(df, po_config)
    return direct_recon


def po_direct_recon(
    df: pd.DataFrame, po_config: PartialOwnershipConfig
) -> pd.DataFrame:
    direct_accounts = (
        df.loc[df["account"].str.contains(po_config.prefix_direct), "account"]
        .drop_duplicates()
        .tolist()
    )
    original_accounts = [
        account.replace(po_config.prefix_direct, "")
        for account in direct_accounts
    ]

    direct_data = df[df["account"].isin(direct_accounts)].rename(
        columns={
            "mv": "po_direct_mv",
            "account": "po_direct_account",
            "units": "po_direct_units",
        }
    )
    direct_data["account"] = direct_data["po_direct_account"].str.replace(
        po_config.prefix_direct, "", regex=False
    )

    original_data = df[df["account"].isin(original_accounts)]
    original_data = original_data.rename(
        columns={"mv": "original_mv", "units": "original_units"}
    )

    recon = pd.merge(
        direct_data,
        original_data,
        on=["date", "account", "instrument"],
        how="outer",
    ).fillna(0)
    recon["mv_diff"] = (recon["po_direct_mv"] - recon["original_mv"]).abs()
    recon["units_diff"] = (
        recon["po_direct_units"] - recon["original_units"]
    ).abs()
    recon = (
        recon[recon["mv_diff"] > po_config.threshold]
        .sort_values(by=["account", "date", "instrument"])
        .reset_index(drop=True)
    )

    cols = [
        "date",
        "account",
        "po_direct_account",
        "instrument",
        "original_units",
        "po_direct_units",
        "units_diff",
        "original_mv",
        "po_direct_mv",
        "mv_diff",
    ]

    return recon[cols]


# endregion


# region Helper functions
def savefile(df: pd.DataFrame, client: str, filename: str):
    save_path = Path(
        "data",
        "outputs",
        "PO_SMA",
        client,
        "Recon",
        filename,
    )
    df.to_csv(save_path, index=False)
    print(f"Saved file: {save_path}")


def read_config(config_path: str) -> Any:
    try:
        config = Binder(Config).parse_toml(config_path)
    except TypeError as e:
        raise MissingConfigError(e)
    return config


# endregion


def main():
    try:
        config = read_config("data/configs/PO_SMA/gresham/po_sma_recon.toml")
    except MissingConfigError as e:
        print(f"Missing configuration error: {e}")
        exit(1)
    tracking_data = get_tracking_data(config)
    if config.sma.recon:
        sma_data = sma_recon(tracking_data, config.sma)
        savefile(sma_data, config.client, "sma_recon.csv")
    if config.partial_ownership.recon:
        po_data = partial_ownership_recon(
            tracking_data, config.partial_ownership
        )
        savefile(po_data, config.client, "po_recon.csv")


if __name__ == "__main__":
    main()
