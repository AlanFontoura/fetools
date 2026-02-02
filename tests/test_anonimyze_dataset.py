import pandas as pd
import pytest
from fetools.utils.anonimyze_dataset import anonimyze_dataset

def test_anonimyze_dataset_args():
    # Setup data
    data = {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "city": ["NY", "LA", "SF"],
        "age": [25, 30, 35]
    }
    df = pd.DataFrame(data)

    # Call with *args
    anon_df, hash_maps = anonimyze_dataset(df, "name", "city")

    # Verification
    assert "name" in anon_df.columns
    assert "city" in anon_df.columns
    
    # Check hash maps
    assert "name" in hash_maps
    assert "city" in hash_maps
    assert isinstance(hash_maps["name"], pd.DataFrame)
    assert isinstance(hash_maps["city"], pd.DataFrame)
    
    # Values should be changed (hashed) - naive check: they shouldn't match originals exactly
    # Since the hashing is random, we can just check if they are still strings and different from input if we assume hash != input
    # But wait, the original code uses random.choices hex string.
    
    # Check that 'name' values are not the original names
    original_names = set(data["name"])
    new_names = set(anon_df["name"])
    # It is extremely unlikely that a random hash matches the original name "Alice"
    assert new_names.isdisjoint(original_names) 

    # Check that 'city' values are not the original cities
    original_cities = set(data["city"])
    new_cities = set(anon_df["city"])
    assert new_cities.isdisjoint(original_cities)

    # Check that 'age' (not anonymized) is preserved
    assert list(anon_df["age"]) == data["age"]
