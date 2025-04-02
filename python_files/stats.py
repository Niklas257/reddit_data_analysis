from create_database import filter_valid_tables
import json
from collections import defaultdict
import numpy as np


# Create row_counts table in stats.db
def create_row_counts_table(con):
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS row_counts (
            table_name TEXT,
            row_count BIGINT
        )
    """
    )

    tables = con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
    ).fetchall()
    print(tables[0])

    tables = filter_valid_tables(tables)

    for table_name in tables:
        # Get count from main database
        row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        # Insert into stats database
        con.execute(f"INSERT INTO row_counts VALUES ('{table_name}', {row_count})")


def create_filtered_row_counts(table, con):
    # Get column names from filtered_threads
    columns = con.execute(
        f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{table}'
    """
    ).fetchall()
    columns = [col[0] for col in columns]  # e.g., ['posts', 'comments_to_posts', ...]

    # Create the filtered_row_counts table
    con.execute(
        f"""
        CREATE OR REPLACE TABLE filtered_row_counts_{table} (
            conversation_length VARCHAR,
            row_count BIGINT
        )
    """
    )

    # Count non-NULL values for each column and insert into filtered_row_counts
    for column in columns:
        count_query = f"""
            SELECT COUNT(*)
            FROM {table}
            WHERE {column} IS NOT NULL
        """
        row_count = con.execute(count_query).fetchone()[0]
        con.execute(
            f"""
            INSERT INTO filtered_row_counts_{table}
            VALUES ('{column}', {row_count})
        """
        )


def analyze_thread_score_distribution(table, con):
    """
    Analyzes the distribution of total scores across threads in the database.
    Uses SQL aggregation for efficiency with large datasets.

    Returns:
        dict: A dictionary where keys are total score values and values are
              the number of threads having that total score.
    """

    # Get all column names from the threads table that might contain IDs
    columns = con.execute(
        f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{table}'
    """
    ).fetchall()
    columns = [col[0] for col in columns]

    # Build a SQL query that joins all relevant tables and calculates total scores in one go
    join_clauses = []
    score_sum_parts = []

    for col in columns:
        # Create a left join for each ID column to its corresponding table
        join_clauses.append(
            f"""
            LEFT JOIN {col} AS {col}_table
            ON {table}.{col} = {col}_table.id
        """
        )

        # Add this table's score to the total sum (handling NULLs)
        score_sum_parts.append(f"COALESCE({col}_table.score, 0)")

    # Combine all parts into a single query
    query = f"""
        SELECT
            ({' + '.join(score_sum_parts)}) AS total_score,
            COUNT(*) AS thread_count
        FROM {table}
        {' '.join(join_clauses)}
        GROUP BY total_score
        ORDER BY total_score
    """

    # Execute the query and convert to dictionary
    results = con.execute(query).fetchall()
    distribution = {row[0]: row[1] for row in results}

    # Save to file with the appropriate key
    try:
        with open("../data/saved_stats.json", "r") as f:
            existing_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        existing_data = {}

    # Add the new data to the existing dictionary
    existing_data[f"thread_score_distribution_{table}"] = distribution

    # Write the updated dictionary back to the file
    with open("../data/saved_stats.json", "w") as f:
        json.dump(existing_data, f)


def get_subreddit_distribution(table, con):
    """
    Analyzes the distribution of subreddits in filtered threads.

    Returns:
        pandas.DataFrame: DataFrame with subreddit names and their post counts,
                         sorted by count in descending order.
    """

    # Join table with posts table to get subreddits
    # Then count occurrences of each subreddit
    distribution = con.execute(
        f"""
        SELECT
            p.subreddit,
            COUNT(*) as number_of_posts
        FROM {table} ft
        JOIN posts p ON ft.posts = p.id
        GROUP BY p.subreddit
        ORDER BY number_of_posts DESC
    """
    ).fetchdf()

    subreddit_dict = dict(
        zip(distribution["subreddit"], distribution["number_of_posts"])
    )
    # Convert any NumPy integers to Python integers
    subreddit_dict = {k: int(v) for k, v in subreddit_dict.items()}
    # Save to file with the appropriate key
    try:
        with open("../data/saved_stats.json", "r") as f:
            existing_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        existing_data = {}

    # Add the new data to the existing dictionary
    existing_data[f"subreddit_distribution_{table}"] = subreddit_dict

    # Write the updated dictionary back to the file
    with open("../data/saved_stats.json", "w") as f:
        json.dump(existing_data, f)


# Get length and width statistics for a lookup table
def table_stats(table, con):

    # Initialize dictionaries with default values of 0
    thread_lengths = defaultdict(int)
    thread_widths = defaultdict(int)
    all_widths = defaultdict(int)

    # Get all data from the table_name table
    result = con.execute(f"SELECT * FROM {table}").fetchall()
    column_names = con.execute(f"PRAGMA table_info({table})").fetchdf()["name"].tolist()

    # Process each row
    for row in result:
        # Count non-null columns in this row, excluding the first column
        non_null_count = sum(1 for value in row[1:] if value is not None)
        thread_lengths[non_null_count] += 1

        max_width = 0
        # Process each non-null column, excluding the first column
        for i, value in enumerate(
            row[1:], start=1
        ):  # start=1 to maintain correct column index
            if value is not None:
                # Assuming the non-null values are lists or can be evaluated as lists
                try:
                    # Parse the list if it's stored as a string
                    if isinstance(value, str):
                        value_list = eval(value)
                    else:
                        value_list = value

                    # Get the length of the list
                    list_length = len(value_list)

                    if list_length > max_width:
                        max_width = list_length

                    # Increment the count for this list length in all_widths
                    all_widths[list_length] += 1

                except Exception:
                    # Handle the case where the value isn't a valid list
                    print(
                        f"Warning: Could not process value in column {column_names[i]}: {value}"
                    )
        # Update thread_widths with the maximum list length for this row
        thread_widths[max_width] += 1

    # Save to file with the appropriate key
    try:
        with open("../data/saved_stats.json", "r") as f:
            existing_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        existing_data = {}

    # Add the new data to the existing dictionary
    existing_data[f"thread_lengths_{table}"] = thread_lengths
    existing_data[f"thread_widths_{table}"] = thread_widths
    existing_data[f"all_widths_{table}"] = all_widths

    # Write the updated dictionary back to the file
    with open("../data/saved_stats.json", "w") as f:
        json.dump(existing_data, f)


def calculate_weighted_average(table):
    with open("../data/saved_stats.json", "r") as f:
        data = json.load(f)
        dictionary = data.get(table, {})

    if len(dictionary) == 0:
        print(f"No data found for {table}")
        return
    weighted_average = sum(
        int(key) * int(value) for key, value in dictionary.items()
    ) / sum(dictionary.values())
    # Save to file with the appropriate key
    # Read the existing data first
    try:
        with open("../data/saved_stats.json", "r") as f:
            existing_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        existing_data = {}

    # Add the new data to the existing dictionary
    existing_data[f"{table}_weighted_average"] = weighted_average

    # Write the updated dictionary back to the file
    with open("../data/saved_stats.json", "w") as f:
        json.dump(existing_data, f)


def get_thread_lengths(table, con):
    df = con.execute(f"SELECT * FROM filtered_row_counts_{table}").fetchdf()
    thread_lengths = {}
    for i in range(len(df) - 1):
        current_count = df.loc[i, "row_count"]
        previous_count = df.loc[i + 1, "row_count"]
        if i == len(df) - 2:
            thread_lengths[i] = current_count - previous_count
            thread_lengths[i + 1] = current_count
        else:
            thread_lengths[i] = current_count - previous_count

    thread_lengths = {
        k: int(v) if isinstance(v, np.int64) else v for k, v in thread_lengths.items()
    }
    # Save to file with the appropriate key
    try:
        with open("../data/saved_stats.json", "r") as f:
            existing_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        existing_data = {}

    # Add the new data to the existing dictionary
    existing_data[f"thread_lengths_{table}"] = thread_lengths

    # Write the updated dictionary back to the file
    with open("../data/saved_stats.json", "w") as f:
        json.dump(existing_data, f)
