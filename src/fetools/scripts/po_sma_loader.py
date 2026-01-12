import pandas as pd


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
    all_ownership_levels = [df]
    current_level = df
    while not current_level.empty:
        current_level = simple_ownership_of_ownership(current_level)
        all_ownership_levels.append(current_level)
    extended_df = pd.concat(all_ownership_levels, ignore_index=True)
    return extended_df


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
    ownership_of_ownership["Owner"] = ownership_of_ownership["Owner_x"]
    ownership_of_ownership["Owned"] = ownership_of_ownership["Owned_y"]
    ownership_of_ownership["Percentage"] = (
        ownership_of_ownership["Percentage_x"]
        * ownership_of_ownership["Percentage_y"]
    )
    ownership_of_ownership["Date"] = [
        max(d1, d2)
        for d1, d2 in zip(
            ownership_of_ownership["Date_x"], ownership_of_ownership["Date_y"]
        )
    ]
    ownership_of_ownership = ownership_of_ownership[
        ["Owner", "Owned", "Date", "Percentage"]
    ]
    return ownership_of_ownership
