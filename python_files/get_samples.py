import pandas as pd
import random
from textwrap import fill
from tqdm import tqdm
import json


def get_random_thread_details(table, con):

    # Get the count of rows in table
    count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    if count == 0:
        print(f"No threads found in {table} table.")
        return None

    # Select a random row from table
    random_offset = random.randint(0, count - 1)
    random_thread = con.execute(
        f"SELECT * FROM {table} OFFSET {random_offset} LIMIT 1"
    ).fetchdf()

    print("Random thread selected:")
    # Display the random thread with better formatting
    for col in random_thread.columns:
        value = random_thread[col].iloc[0]
        if not pd.isna(value):  # Only print if value is not NaN
            print(f"{col}: {value}")
    print("\n" + "=" * 80 + "\n")

    # Get column names to know which tables to query
    columns = random_thread.columns.tolist()

    # For each column that might contain an ID, look up the full information
    for column in columns:
        # Skip columns that don't correspond to tables
        if column in ["id", "created_utc", "score", "num_comments"]:
            continue

        # Get the ID value from the random thread
        id_value = random_thread[column].iloc[0]

        # Skip if the ID is null
        if pd.isna(id_value):
            continue

        # Query the corresponding table for the full information
        try:
            full_info = con.execute(
                f"SELECT * FROM {column} WHERE id = '{id_value}'"
            ).fetchdf()

            if not full_info.empty:
                print(f"Full information for {column} (ID: {id_value}):")

                # Display each column with better formatting for long text
                for col in full_info.columns:
                    value = full_info[col].iloc[0]
                    if pd.isna(value):
                        print(f"{col}: NULL")
                    elif isinstance(value, str) and len(value) > 80:
                        # For long text fields, print with proper formatting
                        print(f"{col}:")
                        wrapped_text = fill(
                            value,
                            width=100,
                            initial_indent="    ",
                            subsequent_indent="    ",
                        )
                        print(wrapped_text)
                        print()
                    else:
                        print(f"{col}: {value}")

                print("\n" + "-" * 80 + "\n")
            else:
                print(f"No information found for {column} with ID {id_value}")
        except Exception as e:
            print(f"Error querying table {column}: {e}")

    return random_thread


def get_thread_with_metadata(con):
    # Get a random thread
    count = con.execute("SELECT COUNT(*) FROM filtered_threads").fetchone()[0]
    random_offset = random.randint(0, count - 1)
    thread = con.execute(
        f"SELECT * FROM filtered_threads OFFSET {random_offset} LIMIT 1"
    ).fetchdf()

    # Create the result dictionary
    result = {}

    # For each column in the thread
    for column in thread.columns:
        id_value = thread[column].iloc[0]

        # Skip if the value is NULL or if it's not an ID column
        if pd.isna(id_value) or column in [
            "id",
            "created_utc",
            "score",
            "num_comments",
        ]:
            result[column] = id_value
            continue

        # Get the full metadata for this ID from its corresponding table
        try:
            metadata = con.execute(
                f"SELECT * FROM {column} WHERE id = '{id_value}'"
            ).fetchdf()
            if not metadata.empty:
                # Convert the metadata to a dictionary
                metadata_dict = metadata.iloc[0].to_dict()
                result[column] = metadata_dict
            else:
                result[column] = None
        except Exception:
            result[column] = None

    return result


def create_thread_json(num_threads=100, output_file="random_threads.jsonl"):
    # Create the JSON Lines file
    with open(output_file, "w", encoding="utf-8") as f:
        for _ in tqdm(range(num_threads), desc="Generating threads"):
            thread_data = get_thread_with_metadata()
            # Write the JSON object as a single line
            json.dump(thread_data, f, ensure_ascii=False)
            f.write("\n")
