import pandas as pd
from pathlib import Path
from fetools import adjust_fco_table, add_accounts_to_fco


def main(
    fco: pd.DataFrame,
    accounts: pd.DataFrame,
    clients: pd.DataFrame,
    cutoff_date: str,
    output_path: Path,
) -> None:
    fco_adjusted = adjust_fco_table(fco, cutoff_date)
    final_fco = add_accounts_to_fco(fco_adjusted, accounts)
    final_fco.to_csv(output_path / "fco_with_sma_accounts.csv", index=False)


if __name__ == "__main__":
    accounts = pd.read_csv("data/inputs/partial_ownership/Account.csv")
    clients = pd.read_csv("data/inputs/partial_ownership/Client.csv")
    fco = pd.read_csv("data/inputs/partial_ownership/LEOwnershipe.csv")
