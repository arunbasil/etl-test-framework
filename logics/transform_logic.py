import pandas as pd


def transform_data(source_file: str, target_file: str) -> None:
    """
    Transforms the data in the source file into the target format and writes it to the target file.
    :param source_file: The file path of the source data file.
    :param target_file: The file path of the target data file.
    :return: None
    """
    # Load the source data into a pandas DataFrame
    source_data = pd.read_csv(source_file)

    # Split the name column into first name and last name columns
    source_data[['first_name', 'last_name']] = source_data['name'].str.split(" ", expand=True)

    # Reorder and rename the columns
    target_data = source_data[['id', 'first_name', 'last_name', 'age']]
    target_data = target_data.rename(
        columns={'id': 'id', 'first_name': 'first_name', 'last_name': 'last_name', 'age': 'age'})

    # Write the target data to a CSV file
    target_data.to_csv(target_file, index=False)
