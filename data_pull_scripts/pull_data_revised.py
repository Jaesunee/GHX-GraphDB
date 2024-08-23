import pandas as pd
import csv
from math import ceil  # Import ceil for batch size calculation
import numpy as np  # Import numpy for filtering non-numeric IDs
from connectSnowflake import pull_data_snowflake

def is_numeric(s):
    """
    Checks if a string can be converted to a number (including float).
    """
    try:
        np.float64(s)
        return True
    except ValueError:
        return False


def extract_relevant_ids(edge_dfs, relationship, name, relevant_ids):
    """
    Extracts relevant IDs based on relationships and filters non-numeric IDs.

    Args:
        edge_dfs: List of DataFrames representing edges.
        relationship: List of indices in edge_dfs for relevant edges.
        name: Name of the column containing IDs.
        relevant_ids: List to accumulate relevant IDs.

    Returns:
        A list of filtered relevant IDs.
    """

    for i in relationship:
        relevant_ids.extend(edge_dfs[i][name].tolist())
    relevant_ids = list(set(relevant_ids))  # Remove duplicates

    # Efficiently filter non-numeric IDs using numpy vectorization
    relevant_ids = relevant_ids[np.vectorize(is_numeric)(relevant_ids)]
    return relevant_ids


def process_table(qry, table_name, output_file, type, parameters, node_dfs, node_names, edge_dfs, edge_names, batch_size=900):
    """
    Processes a table by fetching data in batches, handling different data types,
    and storing results in appropriate structures.

    Args:
        qry: SQL query to fetch data.
        table_name: Name of the table being processed.
        output_file: Name of the output CSV file.
        type: Indicates data type (nodes, informed_edges, or edges).
        parameters: List of query parameters from the original code.
        node_dfs: List to store node DataFrames.
        node_names: List to store node names.
        edge_dfs: List to store edge DataFrames.
        edge_names: List to store edge names.
        batch_size: Size of data batches for fetching data in chunks.
    """

    df = pd.DataFrame()
    parts = name.split(", ")

    if type == "nodes" and parameters[relationship[0]][-1] != "":
        relevant_ids = extract_relevant_ids(edge_dfs, relationship, parts[0], [])

        # Efficient batch processing using vectorized IN clause
        num_batches = ceil(len(relevant_ids) / batch_size)
        batch_ids = np.array_split(relevant_ids, num_batches)
        for batch_ids in batch_ids:
            in_clause = ','.join(map(str, batch_ids))
            batch_qry = qry + f" WHERE MASTER_ID IN ({in_clause}) AND TRY_TO_NUMBER(PRIMARY_EID) IS NOT NULL;"
            batch_df = pull_data_snowflake(batch_qry)

            if name != "":
                batch_df = batch_df.rename(columns=dict(zip(batch_df.columns, parts)))
            batch_df.drop_duplicates(subset=[parts[0]], keep='first', inplace=True)
            df = pd.concat([df, batch_df], ignore_index=True)
            print("batch processed for", parts[0])

    elif type == "informed_edges":
        relevant_ids = extract_relevant_ids(edge_dfs, relationship[1], parts[1], [])

        num_batches = ceil(len(relevant_ids) / batch_size)
        batch_ids = np.array_split(relevant_ids, num_batches)
        for batch_ids in batch_ids:
            in_clause = ','.join(map(str, batch_ids))
            batch_qry = qry + f" WHERE {selection.split(', ')[1]} IN ({in_clause}) LIMIT 100000;"
            batch_df = pull
