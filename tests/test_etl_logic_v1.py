import os
import csv
import pytest
from etl_csv_pytest_project.logics.transform_logic import transform_data

"""
Here are some best practices when using pytest for ETL tests:
:Write test functions that are self-contained, meaning they do not rely on global state, and that test only one thing at a time. This makes it easier to diagnose failures and to understand what is being tested.
:Use parameterization to write tests that can be run against different datasets or configurations. This helps to catch issues that may only occur under certain conditions.
:Use asserts to test the output of your code and to make clear what you expect the code to do.
:Use fixtures to manage test data, setup and teardown. This helps keep the tests clean and ensures that tests are run in the correct order.
:Make use of built-in pytest features, such as marks, to categorize tests and to selectively run tests based on their categorization.
:Here is an example code using pytest to test the ETL process of transforming data from a source CSV file to a target CSV file:
"""

import pytest

@pytest.fixture(scope='session', autouse=True)
def data_transformation(request):
    config = request.config.getoption("--config")
    source_file = config.source_file
    target_file = config.target_file

    # Perform the data transformation here
    # ...

    return source_file, target_file


@pytest.fixture(scope='session')
def source_file():
    file_path = os.path.join(os.path.dirname(__file__), 'source.csv')
    with open(file_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['col1', 'col2', 'col3'])
        writer.writerow([1, 2, 3])
        writer.writerow([4, 5, 6])

    yield file_path

    os.remove(file_path)


@pytest.fixture(scope='session')
def target_file(source_file):
    target_path = os.path.join(os.path.dirname(__file__), 'target.csv')
    transform_data(source_file, target_path)
    yield target_path

    os.remove(target_path)


def test_transform_data_creates_target_file(target_file):
    assert os.path.isfile(target_file)


def test_target_file_contains_transformed_data(target_file):
    with open(target_file, 'r') as f:
        reader = csv.reader(f)
        headers = next(reader)
        assert headers == ['col1', 'col2_squared', 'col3_squared']

        row1 = next(reader)
        assert row1 == [1, 4, 9]

        row2 = next(reader)
        assert row2 == [4, 25, 36]
