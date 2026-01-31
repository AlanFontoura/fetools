import pandas as pd
from pathlib import Path
from dataclass_binder import Binder
from fetools.utils.po_sma_tools import (
    PO_SMA_Config,
    create_structure_files,
    get_ownership_file,
    filter_ownership_by_date,
    create_split_accounts_file,
)
from pprint import pprint


if __name__ == "__main__":
    # Read config file
    config_file = Path("data/inputs/PO_SMA/gresham/po_sma.toml")
    config = Binder(PO_SMA_Config).parse_toml(config_file)

    # Create main structure files
    structure = create_structure_files(
        config.account_file,
        config.type,
    )

    # Create Fund Client Ownership file
    accounts = pd.read_csv(config.account_file)
    ownership = get_ownership_file(config.ownership_file)
    ownership.to_csv(
        "data/outputs/PO_SMA/gresham/fund_client_ownership.csv", index=False
    )

    splits = create_split_accounts_file(
        accounts,
        ownership,
    )
    pprint(splits)

    current_ownership, past_ownership = filter_ownership_by_date(
        ownership,
        config.first_transaction_date,
    )
