import duckdb
import pandas as pd
import re
import gc
import json
from collections import defaultdict
import numpy as np
import ast


# Add initial tables to the database
def add_posts_table(con, posts_file):
    # Create posts table
    query = f"""
    CREATE OR REPLACE TABLE posts AS
    SELECT id, title, selftext, subreddit, score, upvote_ratio, media, author
    FROM read_csv_auto('{posts_file}',
                    null_padding=True,
                    ignore_errors=True)
    -- LIMIT 1000000
    """
    con.execute(query)
    print("posts table created successfully.")


def add_comments_working_table(con, comments_file):
    # Create comments_working table
    batch_size = 1000000  # Adjust based on your needs

    # First create the target table structure
    con.execute("SET threads TO 4")  # Use 4 cores
    con.execute(
        """
    CREATE OR REPLACE TABLE comments_working (
        id VARCHAR,
        body VARCHAR,
        score INTEGER,
        author VARCHAR,
        parent_id VARCHAR
    )
    """
    )

    chunk_counter = 0
    total_rows = 0

    for chunk in pd.read_csv(comments_file, chunksize=batch_size, dtype="str"):
        chunk_counter += 1

        # Explicit memory management
        try:
            # Create and insert batch
            con.execute("CREATE TEMP TABLE batch AS SELECT * FROM chunk")
            con.execute(
                """
            INSERT INTO comments_working
            SELECT id, body, score, author, parent_id
            FROM batch
            """
            )
            rows_inserted = len(chunk)
            total_rows += rows_inserted
            print(
                f"Batch {chunk_counter}: Inserted {rows_inserted} rows (Total: {total_rows})"
            )
            # Manually clean up
            del chunk  # Explicitly delete the DataFrame
            con.execute("DROP TABLE IF EXISTS batch")
            gc.collect()  # Force garbage collection

            # Progress tracking

        except Exception as e:
            print(f"Error processing batch {chunk_counter}: {str(e)}")
            con.execute("DROP TABLE IF EXISTS batch")
            raise

    print(f"Finished loading {total_rows} total rows")


def add_initial_comments_tables(con):
    # Create comments_to_posts table
    con.execute("BEGIN TRANSACTION")

    try:
        # Create the target table if it doesn't exist
        con.execute(
            """
        CREATE OR REPLACE TABLE comments_to_posts (
            id VARCHAR,
            body VARCHAR,
            score INTEGER,
            author VARCHAR,
            parent_id VARCHAR
        )
        """
        )

        # Insert matching records
        con.execute(
            """
        INSERT INTO comments_to_posts
        SELECT id, body, score, author, SUBSTRING(parent_id, 4) AS parent_id
        FROM comments_working
        WHERE SUBSTRING(parent_id, 4) IN (SELECT id FROM posts)
        """
        )
        print("Inserted matching comments into comments_to_posts table successfully.")

        # Delete processed records
        con.execute(
            """
        DELETE FROM comments_working
        WHERE SUBSTRING(parent_id, 4) IN (SELECT id FROM posts)
        """
        )

        con.execute("COMMIT")
        print("Successfully moved and deleted matching comments")

    except Exception as e:
        con.execute("ROLLBACK")
        print(f"Error: {e}")

    # Create comments_to_comments_1 table
    con.execute("BEGIN TRANSACTION")

    try:
        # Create the target table if it doesn't exist
        con.execute(
            """
        CREATE OR REPLACE TABLE comments_to_comments_1 (
            id VARCHAR,
            body VARCHAR,
            score INTEGER,
            author VARCHAR,
            parent_id VARCHAR
        )
        """
        )

        # Insert matching records
        con.execute(
            """
        INSERT INTO comments_to_comments_1
        SELECT id, body, score, author, SUBSTRING(parent_id, 4) AS parent_id
        FROM comments_working
        WHERE SUBSTRING(parent_id, 4) IN (SELECT id FROM comments_to_posts)
        """
        )
        print(
            "Inserted matching comments into comments_to_comments_1 table successfully."
        )

        # Delete processed records
        con.execute(
            """
        DELETE FROM comments_working
        WHERE SUBSTRING(parent_id, 4) IN (SELECT id FROM comments_to_posts)
        """
        )

        con.execute("COMMIT")
        print("Successfully moved and deleted matching comments")

    except Exception as e:
        con.execute("ROLLBACK")
        print(f"Error: {e}")


# Add comments_to_comments tables
def add_comments_to_comments_tables(con):
    current_level = 1
    rows_found = 1  # Initialize with a non-zero value to enter the loop

    while rows_found > 0:
        next_level = current_level + 1

        # Start transaction
        con.execute("BEGIN TRANSACTION")

        try:
            # First, count how many comments we'll process at this level
            count_query = f"""
            SELECT COUNT(*)
            FROM comments_working AS c
            WHERE SUBSTRING(c.parent_id, 4) IN (
                SELECT id FROM comments_to_comments_{current_level}
            )
            """
            rows_found = con.execute(count_query).fetchone()[0]
            print(f"Found {rows_found} comments for level {next_level}")

            if rows_found > 0:
                # Create the next level table with the matching comments
                create_table_query = f"""
                CREATE OR REPLACE TABLE comments_to_comments_{next_level} AS
                SELECT id, body, score, author, SUBSTRING(parent_id, 4) AS parent_id
                FROM comments_working
                WHERE SUBSTRING(parent_id, 4) IN (
                    SELECT id FROM comments_to_comments_{current_level}
                )
                """
                con.execute(create_table_query)

                # Delete the processed rows from comments_working
                delete_query = f"""
                DELETE FROM comments_working
                WHERE SUBSTRING(parent_id, 4) IN (
                    SELECT id FROM comments_to_comments_{current_level}
                )
                """
                con.execute(delete_query)

                # Commit the transaction
                con.execute("COMMIT")
                print(f"Created level {next_level} table and deleted processed rows")
                current_level = next_level
            else:
                print(f"No more nested comments found after level {current_level}")
                con.execute("ROLLBACK")  # No changes needed
                break

        except Exception as e:
            con.execute("ROLLBACK")
            print(f"Error processing level {next_level}: {e}")
            break


def add_initial_tables(con, posts_file, comments_file):
    # Create posts table
    query = f"""
    CREATE OR REPLACE TABLE posts AS
    SELECT id, title, selftext, subreddit, score, upvote_ratio, media, author
    FROM read_csv_auto('{posts_file}',
                    null_padding=True,
                    ignore_errors=True)
    LIMIT 280000
    """
    con.execute(query)

    # Create comments_to_posts table
    query = f"""
    CREATE OR REPLACE TABLE comments_to_posts AS
    SELECT id, body, score, author,
    SUBSTRING(c.parent_id, 4) AS parent_id
    -- c.parent_id AS parent_id
    FROM read_csv_auto('{comments_file}',
                    null_padding=True,
                    ignore_errors=True) AS c
    WHERE SUBSTRING(c.parent_id, 4) IN (
    -- WHERE c.parent_id IN (
    SELECT id FROM posts
    )
    """
    con.execute(query)

    # Create comments_to_comments_1 table
    query = f"""
    CREATE OR REPLACE TABLE comments_to_comments_1 AS
    SELECT id, body, score, author,
    SUBSTRING(c.parent_id, 4) AS parent_id
    -- c.parent_id AS parent_id
    FROM read_csv_auto('{comments_file}',
                    null_padding=True,
                    ignore_errors=True) AS c
    WHERE SUBSTRING(c.parent_id, 4) IN (
    -- WHERE c.parent_id IN (
    SELECT id FROM comments_to_posts
    )
    """
    con.execute(query)


# Add comments_to_comments tables
def add_comments_to_comments_tables_old(con, comments_file):
    current_level = 1
    rows_found = 1  # Initialize with a non-zero value to enter the loop

    while rows_found > 0:
        next_level = current_level + 1

        # Query to find comments referencing the current level
        query = f"""
        SELECT id, body, score, author,
        SUBSTRING(c.parent_id, 4) AS parent_id
        -- c.parent_id AS parent_id
        FROM read_csv_auto('{comments_file}',
                        null_padding=True,
                        ignore_errors=True) AS c
        WHERE SUBSTRING(c.parent_id, 4) IN (
        -- WHERE c.parent_id IN (
        SELECT id FROM comments_to_comments_{current_level}
        )
        """

        # First, count the total rows without loading them all
        count_query = f"""
        SELECT COUNT(*) FROM (
            {query}
        ) as count_table
        """
        rows_found = con.execute(count_query).fetchone()[0]

        print(f"Found {rows_found} comments for level {next_level}")

        if rows_found > 0:
            # Create the next level table
            create_table_query = f"""
            CREATE OR REPLACE TABLE comments_to_comments_{next_level} AS
            {query}
            """
            con.execute(create_table_query)

            print(f"Created comments_to_comments_{next_level} table in database")
            current_level = next_level
        else:
            print(f"No more nested comments found after level {current_level}")
            break


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


# Sort tables in hierarchical order:
# "posts" first, then "comments_to_posts", then "comments_to_comments_1",…
def sort_key(t):
    if t == "posts":
        return (0, 0)
    elif t == "comments_to_posts":
        return (1, 0)
    else:
        num = int(re.search(r"comments_to_comments_(\d+)", t).group(1))
        return (2, num)


# Create lookup table
def create_lookup_table(con):
    tables = con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
    ).fetchall()

    valid_tables = filter_valid_tables(tables)

    valid_tables.sort(key=sort_key)

    print("Valid hierarchical tables:", valid_tables)

    # Build subqueries for each descendant level that aggregate ids by post.
    # For level i (starting at 1), we build a join chain from posts to the i-th table.
    subqueries = []
    # Note: valid_tables[0] is "posts", so we start from index 1.
    for i in range(1, len(valid_tables)):
        table_name = valid_tables[i]
        join_clause = "FROM posts p\n"
        # Build join chain: for level i, join tables[1] ... tables[i]
        for j in range(1, i + 1):
            alias = f"t{j}"
            parent_alias = "p" if j == 1 else f"t{j-1}"
            current_table = valid_tables[j]
            join_clause += f"JOIN {current_table} {alias} ON {alias}.parent_id = {parent_alias}.id\n"
        # Aggregate the ids from the last table in the chain.
        subquery = (
            f"SELECT p.id as post_id, array_agg(t{i}.id) as {table_name}\n"
            f"{join_clause}GROUP BY p.id"
        )
        subqueries.append((table_name, subquery))

    # Build the final query that creates the lookup_table.
    # The final table will have one column for "posts" (the id from posts)
    # plus one column per descendant table (with the aggregated ids).
    final_sql = "CREATE TABLE lookup_table AS\nSELECT p.id as posts"
    for table_name, _ in subqueries:
        final_sql += f", sub_{table_name}.{table_name}"
    final_sql += "\nFROM posts p\n"
    for table_name, subquery in subqueries:
        final_sql += f"LEFT JOIN (\n{subquery}\n) sub_{table_name} ON sub_{table_name}.post_id = p.id\n"

    # Execute the final SQL to create the lookup_table.
    con.execute(final_sql)
    con.commit()

    print("lookup_table created successfully.")


def create_subreddit_tables(con, subreddit):
    query = f"""
    CREATE TABLE IF NOT EXISTS {subreddit}_ids AS
    SELECT id
    FROM posts
    WHERE subreddit = '{subreddit}'
    """
    con.execute(query)

    query = f"""
    CREATE TABLE IF NOT EXISTS {subreddit}_lookup AS
    SELECT *
    FROM lookup_table
    WHERE posts IN (SELECT id FROM {subreddit}_ids)
    """
    con.execute(query)

    query = f"""
    CREATE TABLE IF NOT EXISTS {subreddit}_threads AS
    SELECT *
    FROM threads
    WHERE posts IN (SELECT id FROM {subreddit}_ids)
    """
    con.execute(query)

    query = f"""
    CREATE TABLE IF NOT EXISTS filtered_{subreddit}_threads AS
    SELECT *
    FROM filtered_threads
    WHERE posts IN (SELECT id FROM {subreddit}_ids)
    """
    con.execute(query)


def create_threads_table(con, threads_table):
    columns = con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
    ).fetchall()

    columns = filter_valid_tables(columns)

    columns.sort(key=sort_key)

    create_table_sql = f"""
    CREATE OR REPLACE TABLE {threads_table} (
        {', '.join(f'{col} VARCHAR' for col in columns)}
    )"""
    con.execute(create_table_sql)

    queries = []
    for depth in range(len(columns) - 1, 0, -1):
        # Determine the starting table name based on depth
        if depth == 0:
            starting_table = "posts"
        elif depth == 1:
            starting_table = "comments_to_posts"
        else:
            starting_table = f"comments_to_comments_{depth - 1}"

        # Generate the list of LEFT JOIN clauses
        join_clauses = []
        for current_depth in range(depth, 0, -1):
            parent_depth = current_depth - 1
            if parent_depth == 0:
                parent_table = "posts"
            elif parent_depth == 1:
                parent_table = "comments_to_posts"
            else:
                parent_table = f"comments_to_comments_{parent_depth - 1}"
            join_clauses.append(
                f"LEFT JOIN {parent_table} AS t{parent_depth} "
                f"ON t{current_depth}.parent_id = t{parent_depth}.id"
            )

        # Generate the SELECT clause, setting deeper columns to NULL
        select_parts = []
        for idx, col in enumerate(columns):
            if idx > depth:
                select_parts.append(f"NULL AS {col}")
            else:
                select_parts.append(f"t{idx}.id AS {col}")

        # Construct the full SQL query
        query = f"""
        SELECT {', '.join(select_parts)}
        FROM {starting_table} AS t{depth}
        {' '.join(join_clauses)}
        """
        queries.append(query)

    # Combine all queries with UNION ALL and insert into the threads table
    if queries:
        final_query = f"INSERT INTO {threads_table}\n" + "\nUNION ALL\n".join(queries)
        con.execute(final_query)


def make_threads_unique(con, filtered_table):
    cursor = con.execute("PRAGMA table_info('lookup_table')")
    columns = [row[1] for row in cursor.fetchall()]
    columns_str = ", ".join(columns)

    # Generate the dynamic part of the query for counting non-NULL columns
    non_null_counts = [
        f"CASE WHEN comments_to_comments_{i} IS NOT NULL THEN 1 ELSE 0 END"
        for i in range(1, len(columns) - 1)
    ]
    non_null_counts_str = " + ".join(non_null_counts)

    # Query to select distinct comments_to_posts IDs, keeping only the longest thread
    distinct_threads_query = f"""
    WITH ranked_threads AS (
        SELECT
            *,
            -- Count the number of non-NULL columns in each thread
            (CASE WHEN posts IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN comments_to_posts IS NOT NULL THEN 1 ELSE 0 END +
            {non_null_counts_str}) AS thread_length,
            -- Assign a random number to each thread for tie-breaking
            ROW_NUMBER() OVER (
                PARTITION BY comments_to_posts
                ORDER BY thread_length DESC, RANDOM()
            ) AS random_rank
        FROM all_threads
    )
    SELECT
        {columns_str}
    FROM ranked_threads
    WHERE random_rank = 1  -- Keep only the thread with the highest thread_length, using random_rank for tie-breaking
      AND comments_to_posts IS NOT NULL  -- Apply only when comments_to_posts is not NULL
    ORDER BY comments_to_posts;
    """

    # Execute the query and replace the threads table with the filtered results
    con.execute(
        f"CREATE OR REPLACE TABLE {filtered_table} AS " + distinct_threads_query
    )


def filter_threads(con, table_to_filter):
    # Create a new table for filtered results
    con.execute(
        f"""
        CREATE OR REPLACE TABLE filtered_threads AS
        SELECT * FROM {table_to_filter} WHERE FALSE
    """
    )

    # Get column names (excluding 'posts')
    columns = con.execute(
        f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{table_to_filter}'
          AND column_name != 'posts'
    """
    ).fetchall()
    columns = [
        col[0] for col in columns
    ]  # e.g., ['comments_to_posts', 'comments_to_comments_1', ...]

    # Prefetch all ID-to-author mappings for efficiency
    id_to_author = {}
    id_to_content = {}  # Track content (title, self_text, body) for each ID

    # Fetch posts data
    posts_data = con.execute("SELECT id, author, title, selftext FROM posts").fetchall()
    for id, author, title, selftext in posts_data:
        id_to_author[id] = author
        # Check if any content is [deleted] or [removed]
        id_to_content[id] = (
            author == "[deleted]"
            or author == "[removed]"
            or title == "[deleted]"
            or title == "[removed]"
            or selftext == "[deleted]"
            or selftext == "[removed]"
        )

    # Fetch comments data for each comment table
    for table in columns:
        comments_data = con.execute(f"SELECT id, author, body FROM {table}").fetchall()
        for id, author, body in comments_data:
            id_to_author[id] = author
            # Check if any content is [deleted] or [removed]
            id_to_content[id] = (
                author == "[deleted]"
                or author == "[removed]"
                or body == "[deleted]"
                or body == "[removed]"
            )

    # Fetch all rows from the threads table
    rows = con.execute(f"SELECT * FROM {table_to_filter}").fetchall()

    # Process each row
    valid_rows = []
    for row in rows:
        # Check if any ID in the row has [deleted] or [removed] content
        has_deleted_content = False
        for idx, id_value in enumerate(row):
            if id_value is not None and id_to_content.get(id_value, False):
                has_deleted_content = True
                break

        if has_deleted_content:
            continue  # Skip this row

        authors = {}  # Reset author counts for each row
        # Iterate over columns (skip 'posts' which is the first column)
        for idx, id_value in enumerate(row[1:], start=1):  # Skip index 0 (posts)
            if id_value is None:
                continue
            author = id_to_author.get(id_value)
            if author:
                authors[author] = authors.get(author, 0) + 1

        # Check criteria
        if len(authors) == 3 and all(count >= 2 for count in authors.values()):
            valid_rows.append(row)

    # Batch insert valid rows into filtered_threads
    if valid_rows:
        # Dynamically generate placeholders for the INSERT query
        placeholders = ", ".join(["?"] * (len(columns) + 1))  # +1 for 'posts' column
        con.executemany(
            f"INSERT INTO filtered_threads VALUES ({placeholders})", valid_rows
        )


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
        # Get count
        row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        # Insert into database
        con.execute(f"INSERT INTO row_counts VALUES ('{table_name}', {row_count})")


def get_depth_distribution(table, con):
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


con = duckdb.connect("../data/database_test.db")
create_row_counts_table(con)
df = con.execute("SELECT * FROM row_counts").fetchdf()

df = df.sort_values(by="row_count", ascending=False)
# Pretty-print the DataFrame
print(df.to_string(index=False))
create_lookup_table(con)
table_stats("lookup_table", con)
calculate_weighted_average("thread_lengths_lookup_table")
calculate_weighted_average("thread_widths_lookup_table")
calculate_weighted_average("all_widths_lookup_table")
create_threads_table(con, "all_threads")
make_threads_unique(con, "threads")
filter_threads(con, "threads")
get_depth_distribution("filtered_threads", con)
get_thread_lengths("filtered_threads", con)
get_thread_score_distribution("threads", con)
get_thread_score_distribution("filtered_threads", con)
get_subreddit_distribution("threads", con)
get_subreddit_distribution("filtered_threads", con)
subreddits = ["AskReddit", "memes", "politics"]
for subreddit in subreddits:
    create_subreddit_tables(con, subreddit)
    table_stats(f"{subreddit}_lookup", con)
    calculate_weighted_average(f"thread_lengths_{subreddit}_lookup")
    calculate_weighted_average(f"thread_widths_{subreddit}_lookup")
    calculate_weighted_average(f"all_widths_{subreddit}_lookup")
    table_stats(f"{subreddit}_threads", con)
    table_stats(f"filtered_{subreddit}_threads", con)
