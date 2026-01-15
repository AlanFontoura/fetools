import pandas as pd
import tomllib
from fetools.scripts.base_main import ReportGeneric
from pprint import pprint


class ComplianceReport(ReportGeneric):
    def __init__(self, config_file: str = ""):
        super().__init__()
        with open(config_file, "rb") as f:
            self.config = tomllib.load(f)

    @property
    def guidelines_path(self) -> str:
        return f"s3://d1g1t-client-{self.config['region']}/{self.config['client']}/exports/{self.config['env']}-{self.config['client']}-investment-mandates-guideline-limits.csv"

    def test(self) -> None:
        pprint(self.config)


if __name__ == "__main__":
    crm = ComplianceReport(
        config_file="data/inputs/compliance/compliance.toml"
    )
    crm.test()
