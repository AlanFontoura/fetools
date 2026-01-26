from dataclasses import dataclass

# from danoan.toml_dataclass import TomlDataClassIO
from dataclass_binder import Binder
from pathlib import Path
from pprint import pprint


@dataclass
class Items:
    filter_criterion: str
    url: str
    value: str


@dataclass
class FilterSets:
    items: list[Items]


@dataclass
class BaseConfig:
    server: str
    user: str
    entrypoint: str
    date: str
    date_range: str
    freq: str
    currency: str
    level: str


@dataclass
class Config:
    base: BaseConfig
    filter_sets: FilterSets


def load_config(toml_path: str) -> Config:
    return Binder(Config).parse_toml(Path(toml_path))


if __name__ == "__main__":
    # config = load_config("data/inputs/toml_to_dataclass.toml")
    config = load_config("data/inputs/toml_to_dataclass.toml")
    pprint(config)
    pass
