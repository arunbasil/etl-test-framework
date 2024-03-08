import pandas as pd
import os
import pytest
import etl_csv_pytest_project.logics.transform_logic as l

src_file_path = "/Users/arun/PycharmProjects/pythonProject/etl_csv_pytest_project/data/source_data/source.csv"
src_folder_path = "/Users/arun/PycharmProjects/pythonProject/etl_csv_pytest_project/data/source_data"
target_folder_path = "/Users/arun/PycharmProjects/pythonProject/etl_csv_pytest_project/data/target_data"
target_file_path = "/Users/arun/PycharmProjects/pythonProject/etl_csv_pytest_project/data/target_data/target.csv"

# @pytest.fixture(scope='session', autouse=True)
# def data_transformation(request):
#     config = request.config.getoption("--config")
#     source_file = config.source_file
#     target_file = config.target_file
#     l.transform_data(source_file, target_file)

def test_data_transformation_setup():
    try:
        l.transform_data(src_file_path, target_file_path)
    except Exception as e:
        pytest.fail(f"Data transformation failed: {e}")


def test_data_transformation():
    # Load the source and target data into pandas DataFrames
    source_data = pd.read_csv(src_file_path)
    target_data = pd.read_csv(target_file_path)

    # Check that the source and target data have the same number of rows after transformation
    assert source_data.shape[0] == target_data.shape[
        0], "Data transformation failed: number of rows in source and target data do not match"


def test_target_schema():
    # Load the target data into a pandas DataFrame
    target_data = pd.read_csv(target_file_path)

    # Check that the target data has the correct columns
    expected_columns = ["id","first_name","last_name","age"]
    assert set(target_data.columns) == set(expected_columns), "Target data has incorrect columns"

    # Check that the target data has the correct datatypes
    expected_datatypes = {
        "id": int,
        "first_name": str,
        "last_name":str,
        "age": int
    }
    for column, datatype in expected_datatypes.items():
        assert target_data[column].dtype == datatype, f"{column} has incorrect datatype"


def test_data_quality():
    # Load the target data into a pandas DataFrame
    target_data = pd.read_csv(target_file_path)

    # Check that the data quality rules are being enforced
    if "id" in target_data.columns:
        assert (target_data["id"] > 0).all(), "All values in column_1 should be greater than 0"
    else:
        raise KeyError("column_1 not found in target_data")

    try:
        assert (target_data["age"] >= 0).all(), "Values in Age column are not all greater than 0."
    except AssertionError as e:
        print(f"Assertion Error: {e}")
"""
The code assert (target_data["column_1"] > 0).all() performs an assertion check on a Pandas DataFrame target_data. The purpose of this check is to ensure that all the values in the "column_1" column are greater than 0.
The expression target_data["column_1"] > 0 creates a Boolean mask that is True for all the values in "column_1" that are greater than 0, and False for those that are not. The method .all() is then called on this Boolean mask, which returns True if all elements are True, and False if any of the elements are False.
The assert statement checks if the result of the expression (target_data["column_1"] > 0).all() is True. If it is, the code continues to run. If the result is False, the assert statement raises an AssertionError and the code execution stops.
This check can be useful for verifying that the data in the "column_1" column meets some criteria (in this case, that all values are greater than 0) before further processing."""
