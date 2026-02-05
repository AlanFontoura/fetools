import pandas as pd
from pathlib import Path
from dataclass_binder import Binder
from fetools.utils.po_sma_tools import (
    PO_SMA_Config,
    create_structure_files,
    create_partial_ownership_loaders,
)
from pprint import pprint


if __name__ == "__main__":
    # Read config file
    config_file = Path("data/configs/PO_SMA/gresham/po_sma.toml")
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
