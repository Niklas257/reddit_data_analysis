import json
from collections import defaultdict
import numpy as np
import ast
import os
import psutil
import datetime
import re
import math


# Get only tables that contain all the information (not only the ids)
def filter_valid_tables(tables):
    valid_tables = []
    for t in tables:
        t = t[0]
        if t == "posts":
            valid_tables.append(t)
        elif t == "comments_to_posts":
            valid_tables.append(t)
        else:
            m = re.match(r"comments_to_comments_(\d+)$", t)
            if m:
                if 1 <= int(m.group(1)):
                    valid_tables.append(t)
    return valid_tables


def get_resource_usage():
    """Get current resource usage for this process"""
    process = psutil.Process(os.getpid())

    # Get CPU usage (%) - this is per core, so 100% on 8 cores would be 800%
    cpu_percent = process.cpu_percent(interval=0.1)

    # Get memory usage (MB)
    memory_mb = process.memory_info().rss / (1024 * 1024)

    # Get number of threads being used
    thread_count = process.num_threads()

    return {
        "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "cpu_percent": cpu_percent,
        "memory_mb": memory_mb,
        "thread_count": thread_count,
    }


def log_with_resources(message):
    """Print a message along with current resource usage"""
    resources = get_resource_usage()

    print(
        f"[{resources['timestamp']}] {message} | "
        f"CPU: {resources['cpu_percent']:.1f}% | "
        f"Mem: {resources['memory_mb']:.1f} MB | "
        f"Threads: {resources['thread_count']}"
    )


# Create row_counts table in stats.db
def create_row_counts_table(con):
    con.execute(
        """
        CREATE OR REPLACE TABLE row_counts (
            table_name TEXT,
            row_count BIGINT
        )
    """
    )

    tables = con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
    ).fetchall()

    tables = filter_valid_tables(tables)
    row_counts = {}
    for table_name in tables:
        # Get count
        row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        # Insert into database
        row_counts[table_name] = row_count
        con.execute(f"INSERT INTO row_counts VALUES ('{table_name}', {row_count})")

    # Add the table with table_names as keys and row_counts as values
    # to the saved_stats.json file
    try:
        with open("../data/saved_stats.json", "r") as f:
            existing_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        existing_data = {}
    # Add the new data to the existing dictionary
    existing_data["row_counts_unfiltered"] = row_counts
    # Write the updated dictionary back to the file
    with open("../data/saved_stats.json", "w") as f:
        json.dump(existing_data, f)
    log_with_resources("Row counts table created and saved to file.")


def get_number_of_threads(table, con):
    row_count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    # Save to file with the appropriate key
    try:
        with open("../data/saved_stats.json", "r") as f:
            existing_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        existing_data = {}

    # Add the new data to the existing dictionary
    existing_data[f"number_of_threads_{table}"] = row_count

    # Write the updated dictionary back to the file
    with open("../data/saved_stats.json", "w") as f:
        json.dump(existing_data, f)
    log_with_resources(f"Number of threads for {table} saved to file.")


def get_depth_distribution(table, con):
    # Get column names from table
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
        CREATE OR REPLACE TABLE depth_distribution_{table} (
            conversation_length VARCHAR,
            row_count BIGINT
        )
    """
    )

    # Count distinct non-NULL values for each column and insert into depth_distribution table
    row_counts = {}
    for column in columns:
        count_query = f"""
        SELECT COUNT(DISTINCT {column})
        FROM {table}
        WHERE {column} IS NOT NULL
        """
        row_count = con.execute(count_query).fetchone()[0]
        row_counts[column] = row_count
        con.execute(
            f"""
            INSERT INTO depth_distribution_{table}
            VALUES ('{column}', {row_count})
        """
        )

    try:
        with open("../data/saved_stats.json", "r") as f:
            existing_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        existing_data = {}
    # Add the new data to the existing dictionary
    existing_data[f"depth_distribution_{table}"] = row_counts
    # Write the updated dictionary back to the file
    with open("../data/saved_stats.json", "w") as f:
        json.dump(existing_data, f)
    log_with_resources(f"Depth distribution for {table} saved to file.")


def get_thread_score_distribution(table, con):
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
    # Remove the 'posts' column if it exists
    if "posts" in columns:
        columns.remove("posts")

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
    log_with_resources(f"Thread score distribution for {table} saved to file.")


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
    log_with_resources(f"Subreddit distribution for {table} saved to file.")


# Get length and width statistics for a lookup table
def table_stats(table, con):
    # Initialize dictionaries with default values of 0
    thread_lengths = defaultdict(int)
    thread_widths = defaultdict(int)
    all_widths = defaultdict(int)

    # Get all data from the table_name table
    result = con.execute(f"SELECT * FROM {table}").fetchall()
    column_names = con.execute(f"PRAGMA table_info({table})").fetchdf()["name"].tolist()

    # Check if this is a lookup table (values are lists) or a regular table (values are strings)
    sample_row = result[0] if result else None
    is_lookup_table = False
    if sample_row:
        for value in sample_row[1:]:  # Skip first column
            if value and isinstance(value, str) and value.startswith("["):
                is_lookup_table = True
                break

    # Process each row
    for row in result:
        # Count non-null columns in this row, excluding the first column
        non_null_count = sum(1 for value in row[1:] if value is not None)
        thread_lengths[non_null_count] += 1

        max_width = 0
        # Process each non-null column, excluding the first column
        for i, value in enumerate(row[1:], start=1):
            if value is not None:
                if is_lookup_table:
                    # Handle lookup table format (lists)
                    try:
                        if isinstance(value, str):
                            value_list = ast.literal_eval(value)
                        else:
                            value_list = value
                        list_length = len(value_list)
                        if list_length > max_width:
                            max_width = list_length
                        all_widths[list_length] += 1
                    except Exception:
                        print(
                            f"Warning: Could not process list value in column {column_names[i]}: {value}"
                        )
                else:
                    # Handle regular table format (single values)
                    max_width = max(max_width, 1)
                    all_widths[1] += 1

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

    log_with_resources(
        f"Thread lengths and widths statistics for {table} saved to file."
    )


def calculate_weighted_average(table):
    with open("../data/saved_stats.json", "r") as f:
        data = json.load(f)
        dictionary = data.get(table, {})

    if len(dictionary) == 0:
        print(f"No data found for {table}")
        return
    if "depth" in table:
        weighted_average = sum(
            idx * int(value) for idx, (key, value) in enumerate(dictionary.items())
        ) / (sum(v for v in dictionary.values() if v > 0) + 1e-4)
    elif "score" in table:
        weighted_average = sum(
            int(key) * int(value) for key, value in dictionary.items()
        ) / (sum(dictionary.values()) + 1e-4)
    else:
        weighted_average = sum(
            int(key) * int(value) for key, value in dictionary.items()
        ) / (sum(v for v in dictionary.values() if v > 0) + 1e-4)
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

    log_with_resources(f"Weighted average for {table} calculated and saved to file")


def calculate_variance(table):
    with open("../data/saved_stats.json", "r") as f:
        data = json.load(f)
        dictionary = data.get(table, {})

    if len(dictionary) == 0:
        print(f"No data found for {table}")
        return

    variance = {}
    keys = [key for key in dictionary.keys()]

    # Calculate frequencies (weights) and expand the dataset for percentiles
    expanded_data = []
    for key in keys:
        count = dictionary[key]
        if count > 0:
            expanded_data.extend([key] * count)  # Repeat key 'count' times

    # Compute Q1, Q2, Q3, and IQR
    q1 = expanded_data[math.ceil(0.25 * len(expanded_data) - 1)]
    try:
        q2 = (
            (
                int(expanded_data[int(0.5 * len(expanded_data) - 1)])
                + int(expanded_data[int(0.5 * len(expanded_data))])
            )
            / 2
            if len(expanded_data) % 2 == 0
            else expanded_data[int(0.5 * len(expanded_data))]
        )
    except ValueError:
        q2 = (
            f"Median between {expanded_data[int(0.5 * len(expanded_data) - 1)]} and {expanded_data[int(0.5 * len(expanded_data))]}"
            if (
                len(expanded_data) % 2 == 0
                and (
                    expanded_data[int(0.5 * len(expanded_data) - 1)]
                    != expanded_data[int(0.5 * len(expanded_data))]
                )
            )
            else expanded_data[int(0.5 * len(expanded_data))]
        )
    q3 = expanded_data[math.ceil(0.75 * len(expanded_data) - 1)]
    # If q3 and q1 can be cast to int, caluculate iqr
    try:
        q1 = int(q1)
        q3 = int(q3)
        iqr = int(q3 - q1)
    except ValueError:
        iqr = f"{q3} - {q1}"

    variance["Q1"] = q1
    variance["Q2"] = q2
    variance["Q3"] = q3
    variance["IQR"] = iqr

    try:
        with open("../data/saved_stats.json", "r") as f:
            existing_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        existing_data = {}
    # Add the new data to the existing dictionary
    existing_data[f"{table}_variance"] = variance

    # Write the updated dictionary back to the file
    with open("../data/saved_stats.json", "w") as f:
        json.dump(existing_data, f)

    log_with_resources(f"Variance for {table} calculated and saved to file")


def get_thread_lengths(table, con):
    df = con.execute(f"SELECT * FROM depth_distribution_{table}").fetchdf()
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

    log_with_resources(f"Thread lengths for {table} calculated and saved to file.")


def get_author_distribution(table, con):
    """
    Analyzes the distribution of unique authors per thread in filtered threads using efficient DuckDB queries.

    Returns:
        pandas.DataFrame: DataFrame with number of authors and how many threads have that many authors,
                         sorted by number of authors.
    """
    # Retrieve all comment columns from table
    columns = con.execute(
        f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{table}'
        AND column_name LIKE 'comments_%'
    """
    ).fetchall()
    columns = [col[0] for col in columns]

    # Prepare parts of the UNION ALL query for all authors (comments only)
    parts = []

    # Add comment columns parts
    for col in columns:
        part = f"""
            SELECT ft.comments_to_posts, COALESCE(c_{col}.author, 'NULL_AUTHOR') AS author
            FROM {table} ft
            LEFT JOIN {col} c_{col} ON ft.{col} = c_{col}.id
            WHERE ft.{col} IS NOT NULL
        """
        parts.append(part)

    # Combine all parts with UNION ALL
    all_authors_query = " UNION ALL ".join(parts)

    # Full query to compute author distribution
    full_query = f"""
        WITH all_authors AS (
            {all_authors_query}
        ),
        thread_author_counts AS (
            SELECT
                ft.comments_to_posts,
                COUNT(DISTINCT aa.author) AS num_authors
            FROM {table} ft
            LEFT JOIN all_authors aa ON ft.comments_to_posts = aa.comments_to_posts
            GROUP BY ft.comments_to_posts
        )
        SELECT
            num_authors AS number_of_authors,
            COUNT(comments_to_posts) AS number_of_threads
        FROM thread_author_counts
        GROUP BY num_authors
        ORDER BY number_of_authors
    """

    # Execute the query and fetch results
    distribution = con.execute(full_query).fetchdf()

    try:
        with open("../data/saved_stats.json", "r") as f:
            existing_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        existing_data = {}
    # Convert the DataFrame to a dictionary
    author_distribution = dict(
        zip(distribution["number_of_authors"], distribution["number_of_threads"])
    )
    # Convert any NumPy integers to Python integers
    author_distribution = {
        k: int(v) if isinstance(v, np.int64) else v
        for k, v in author_distribution.items()
    }
    # Add the new data to the existing dictionary
    existing_data[f"author_distribution_{table}"] = author_distribution
    # Write the updated dictionary back to the file
    with open("../data/saved_stats.json", "w") as f:
        json.dump(existing_data, f)
    log_with_resources(f"Author distribution for {table} calculated and saved to file.")
