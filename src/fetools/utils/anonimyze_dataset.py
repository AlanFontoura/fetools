import pandas as pd
import random


def anonimyze_dataset(
    df: pd.DataFrame,
    *cols_to_anonimyze: str,
) -> pd.DataFrame:
    for col in cols_to_anonimyze:
        df = anonimyze_column(df, col)
    return df


def anonimyze_column(
    df: pd.DataFrame,
    col_name: str,
) -> pd.DataFrame:
    hash_map = create_hash_map(df[col_name])
    return (
        df.merge(
            hash_map,
            how="left",
            left_on=col_name,
            right_on=col_name,
        )
        .drop(columns=[col_name])
        .rename(columns={f"new_{col_name}": col_name})
    )


def create_hash_map(col: pd.Series) -> pd.DataFrame:
    unique_values = col.unique().tolist()
    n = len(unique_values)
    return pd.DataFrame(
        {
            col.name: unique_values,
            f"new_{col.name}": generate_hash(n=n),
        }
    )


def generate_hash(n: int, k: int = 16) -> list[str]:
    return [
        "".join(random.choices("0123456789ABCDEF", k=k)) for _ in range(n)
    ]


if __name__ == "__main__":
    df = pd.read_csv("sample_data.csv")
    anonimyzed_df = anonimyze_dataset(df, "name", "email")
