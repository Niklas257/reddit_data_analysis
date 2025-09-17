import json
import re
from time import time
import threading
import duckdb
import pandas as pd
import gc
from stats import log_with_resources, create_row_counts_table
from langdetect import detect
import random


def cascading_comment_deletion(con, starting_level):
    """
    Performs cascading deletion starting from deepest level
    all the way up to posts table, saving and using parent_ids at each step.

    Args:
        db_path: Path to the database.db database
    """
    current_level = starting_level
    parent_ids = []

    print(f"Starting cascading deletion from comments_to_comments_{current_level}")

    # Process comments_to_comments tables from starting_level down to 1
    while current_level >= 1:
        table_name = f"comments_to_comments_{current_level}"

        # For the first table, get all parent_ids
        if current_level == starting_level:
            query = f"SELECT parent_id FROM {table_name}"
            parent_ids = con.execute(query).fetchall()
            parent_ids = [pid[0] for pid in parent_ids]  # Flatten the list

            # Delete all records from this table
            con.execute(f"DELETE FROM {table_name}")
            # Drop the table
            con.execute(f"DROP TABLE IF EXISTS {table_name}")
            print(f"Deleted all comments from {table_name}")
            print(f"Saved {len(parent_ids)} parent_ids to process in the next level")

        # For subsequent tables, find matching ids and get their parent_ids
        else:
            if not parent_ids:
                print(
                    f"No parent_ids to process for {table_name}, moving to next level"
                )
            else:
                # Convert parent_ids to a string for SQL IN clause
                parent_ids_str = ", ".join([f"'{pid}'" for pid in parent_ids])

                # Get new parent_ids where id matches previously saved parent_ids
                query = (
                    f"SELECT parent_id FROM {table_name} WHERE id IN ({parent_ids_str})"
                )
                new_parent_ids = con.execute(query).fetchall()
                new_parent_ids = [pid[0] for pid in new_parent_ids]  # Flatten the list

                # Delete records with matching ids
                con.execute(f"DELETE FROM {table_name} WHERE id IN ({parent_ids_str})")

                # Delete table if empty
                if not new_parent_ids:
                    con.execute(f"DROP TABLE IF EXISTS {table_name}")
                    print(f"Dropped empty table {table_name}")

                print(f"Deleted {len(parent_ids)} comments from {table_name}")

                # Update parent_ids for next iteration
                parent_ids = new_parent_ids
                print(
                    f"Saved {len(parent_ids)} parent_ids to process in the next level"
                )

        # Move to the next level down
        current_level -= 1

    # Finally, process the posts table
    if parent_ids:
        parent_ids_str = ", ".join([f"'{pid}'" for pid in parent_ids])

        # Delete posts with matching ids
        con.execute(f"DELETE FROM comments_to_posts WHERE id IN ({parent_ids_str})")
        print(
            f"Deleted {len(parent_ids)} comments to posts from the comments_to_posts table"
        )
    else:
        print("No posts to delete")
    print("Cascading deletion completed successfully")
    con.commit()


def add_initial_tables(con, posts_file, comments_file, limit=0):
    # Create posts table
    query = f"""
    CREATE OR REPLACE TABLE posts AS
    SELECT id, title, selftext, subreddit, score, upvote_ratio, media, author
    FROM read_csv_auto('{posts_file}',
                    null_padding=True,
                    ignore_errors=True)
    LIMIT {limit if limit > 0 else '' }
    """
    con.execute(query)
    log_with_resources("posts table created successfully.")

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
    log_with_resources("comments_to_posts table created successfully.")

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
    con.commit()
    log_with_resources("comments_to_comments_1 table created successfully.")


# Add comments_to_comments tables
def add_comments_to_comments_tables(con, comments_file):
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

        # Create the next level table
        create_table_query = f"""
        CREATE OR REPLACE TABLE comments_to_comments_{next_level} AS
        {query}
        """
        con.execute(create_table_query)
        con.commit()
        count_query = f"""
        SELECT COUNT(*) FROM comments_to_comments_{next_level}
        """
        rows_found = con.execute(count_query).fetchone()[0]
        if rows_found < 100:
            print("Less than 100 rows found, stopping the process")
            cascading_comment_deletion(con, next_level)
            break

        print(
            f"Created comments_to_comments_{next_level} table in database with {rows_found} rows"
        )
        current_level = next_level


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
        match = re.search(r"comments_to_comments_(\d+)", t)
        if match:
            num = int(match.group(1))
            return (2, num)
        # If the table name doesn't match any expected pattern, put it at the front
        return (-1, 0)


def create_lookup_table(con):
    # Retrieve list of tables from the main schema.
    tables = con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
    ).fetchall()

    valid_tables = filter_valid_tables(tables)
    valid_tables.sort(key=sort_key)

    # Build subqueries for each descendant level that aggregate ids by post.
    # Note: valid_tables[0] is assumed to be "posts", so we start from index 1.
    subqueries = []
    for i in range(1, len(valid_tables)):
        table_name = valid_tables[i]
        join_clause = "FROM posts p\n"
        # Build the join chain: for level i, join tables[1] ... tables[i].
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

    # Materialize each subquery into its own temporary table.
    for table_name, subquery in subqueries:
        temp_sql = f"CREATE TEMP TABLE temp_{table_name} AS\n{subquery}"
        con.execute(temp_sql)
        con.commit()

    # Start the incremental join by materializing the base posts table into an intermediate table.
    con.execute(
        "CREATE TEMP TABLE intermediate_lookup AS SELECT id AS posts FROM posts"
    )
    con.commit()

    # Now join each temporary table one at a time with the current intermediate table.
    for table_name, _ in subqueries:
        join_sql = f"""
        CREATE TEMP TABLE new_intermediate AS
        SELECT i.*, t.{table_name}
        FROM intermediate_lookup i
        LEFT JOIN temp_{table_name} t ON i.posts = t.post_id
        """
        con.execute(join_sql)
        con.commit()

        # Drop the old intermediate table and replace it with the new one.
        con.execute("DROP TABLE intermediate_lookup")
        con.commit()
        con.execute("ALTER TABLE new_intermediate RENAME TO intermediate_lookup")
        con.commit()

    # Create the final lookup table from the fully joined intermediate table.
    con.execute(
        "CREATE OR REPLACE TABLE lookup_table AS SELECT * FROM intermediate_lookup"
    )
    con.commit()
    log_with_resources("lookup_table created successfully.")

    # Clean up: drop the temporary tables as they are no longer needed.
    for table_name, _ in subqueries:
        drop_sql = f"DROP TABLE IF EXISTS temp_{table_name}"
        con.execute(drop_sql)
        con.commit()
        log_with_resources(f"Temp table {table_name} dropped")

    # Also drop the remaining intermediate table.
    con.execute("DROP TABLE IF EXISTS intermediate_lookup")
    con.commit()


def create_subreddit_tables(con, subreddit, threads_table="threads"):
    query = f"""
    CREATE OR REPLACE TABLE {subreddit}_ids AS
    SELECT id
    FROM posts
    WHERE subreddit = '{subreddit}'
    """
    con.execute(query)

    query = f"""
    CREATE OR REPLACE TABLE {subreddit}_lookup AS
    SELECT *
    FROM lookup_table
    WHERE posts IN (SELECT id FROM {subreddit}_ids)
    """
    con.execute(query)

    query = f"""
    CREATE OR REPLACE TABLE {subreddit}_{threads_table} AS
    SELECT *
    FROM {threads_table}
    WHERE posts IN (SELECT id FROM {subreddit}_ids)
    """
    con.execute(query)
    log_with_resources(f"Created tables for subreddit: {subreddit} successfully.")


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

    log_with_resources(f"Created {threads_table} table successfully.")


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
    log_with_resources(
        f"Filtered {filtered_table} to keep only the longest thread for each comments_to_posts ID."
    )


def filter_threads(
    con,
    table_to_filter,
    new_table,
    num_authors=None,
    min_authors=False,
    check_english=False,
):
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
            is_deleted_or_removed = (
                author == "[deleted]"
                or author == "[removed]"
                or body == "[deleted]"
                or body == "[removed]"
            )
            is_not_english = False
            if (
                check_english
                and table == "comments_to_posts"
                and not is_deleted_or_removed
            ):
                # Check if the content is in English
                try:
                    lang = detect(body)
                    if lang != "en":
                        is_not_english = True
                except Exception:
                    is_not_english = True
            id_to_content[id] = is_deleted_or_removed or is_not_english

    if num_authors is None:
        # Direct deletion approach for the simple filtering case
        # Create a temporary table to store IDs of rows to delete
        con.execute(
            "CREATE TEMPORARY TABLE IF NOT EXISTS rows_to_delete (row_id VARCHAR)"
        )

        # Fetch rows and identify ones with deleted content
        rows = con.execute(
            f"SELECT comments_to_posts, * FROM {table_to_filter}"
        ).fetchall()
        rows_to_delete = []

        for row in rows:
            pk_value = row[0]  # The primary key value
            # Check if any ID in the row has [deleted] or [removed] content
            has_deleted_content = False
            for id_value in row[1:]:  # Skip the primary key
                if id_value is not None and id_to_content.get(id_value, False):
                    has_deleted_content = True
                    break

            if has_deleted_content:
                rows_to_delete.append((pk_value,))

        # Batch delete rows with deleted content
        if rows_to_delete:
            con.executemany("INSERT INTO rows_to_delete VALUES (?)", rows_to_delete)
            count_before = con.execute(
                f"SELECT COUNT(*) FROM {table_to_filter}"
            ).fetchone()[0]
            con.execute(
                f"DELETE FROM {table_to_filter} WHERE comments_to_posts IN (SELECT row_id FROM rows_to_delete)"
            )
            count_after = con.execute(
                f"SELECT COUNT(*) FROM {table_to_filter}"
            ).fetchone()[0]
            log_with_resources(
                f"Deleted {count_before - count_after} rows with deleted content from {table_to_filter}."
            )

        # Drop temporary table
        con.execute("DROP TABLE IF EXISTS rows_to_delete")

    else:
        # For num_authors case, use the original approach with a new table
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {new_table} AS
            SELECT * FROM {table_to_filter} WHERE FALSE
            """
        )

        # Fetch all rows from the threads table
        rows = con.execute(f"SELECT * FROM {table_to_filter}").fetchall()

        # Process each row
        valid_rows = []
        for row in rows:
            # Check if any ID in the row has [deleted] or [removed] content
            has_deleted_content = False
            for id_value in row:
                if id_value is not None and id_to_content.get(id_value, False):
                    has_deleted_content = True
                    break

            if has_deleted_content:
                continue  # Skip this row

            # Check author criteria
            authors = {}  # Reset author counts for each row
            # Iterate over columns (skip 'posts' which is the first column)
            for id_value in row[1:]:  # Skip index 0 (posts)
                if id_value is None:
                    continue
                author = id_to_author.get(id_value)
                if author:
                    authors[author] = authors.get(author, 0) + 1

            # Check criteria
            if min_authors:
                # For min_authors, check if there are at least num_authors unique authors
                if len(authors) >= num_authors:
                    valid_rows.append(row)
            else:
                if len(authors) == num_authors and all(
                    count >= 2 for count in authors.values()
                ):
                    valid_rows.append(row)

        # Batch insert valid rows into new_table
        if valid_rows:
            # Dynamically generate placeholders for the INSERT query
            placeholders = ", ".join(
                ["?"] * (len(columns) + 1)
            )  # +1 for 'posts' column
            con.executemany(
                f"INSERT INTO {new_table} VALUES ({placeholders})", valid_rows
            )
            log_with_resources(
                f"Created {new_table} with {len(valid_rows)} valid rows."
            )


def filter_by_score(con, table_to_filter):
    """
    Create a subset of threads with post score above 1000 and a subset of threads with post score below 1000.
    """
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {table_to_filter}_viral AS
        SELECT *
        FROM {table_to_filter}
        WHERE posts IN (SELECT id FROM posts WHERE score >= 1000)
        """
    )
    log_with_resources(f"Created {table_to_filter}_viral table")
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {table_to_filter}_non_viral AS
        SELECT *
        FROM {table_to_filter}
        WHERE posts IN (SELECT id FROM posts WHERE score < 1000)
        """
    )
    log_with_resources(f"Created {table_to_filter}_non_viral table")


def create_testing_threads(
    con, table_to_filter, new_table, num_threads_per_category=20
):
    log_with_resources(f"Starting creation of {new_table}...")
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {new_table} AS
        SELECT * FROM {table_to_filter} WHERE FALSE
        """
    )

    # List to store all comments_to_posts IDs that will be moved
    all_ids_to_move = []

    # Define the static source tables
    source_tables = [
        "threads_2_authors",
        "threads_3_authors",
        "threads_4_authors",
        "threads_5_authors",
        "threads_viral",
        "threads_non_viral",
    ]
    subreddit_tables = [
        "AskReddit_threads",
        "memes_threads",
        "distantsocializing_threads",
        "ACTrade_threads",
        "RedditSessions_threads",
        "AmItheAsshole_threads",
        "wallstreetbets_threads",
        "politics_threads",
        "teenagers_threads",
        "AnimalCrossing_threads",
    ]

    for source_tbl in source_tables:
        try:
            eligible_ids_query = f"""
                SELECT t1.comments_to_posts
                FROM {source_tbl} AS t1
                JOIN {table_to_filter} AS t2
                ON t1.comments_to_posts = t2.comments_to_posts
            """
            eligible_ids = [
                row[0] for row in con.execute(eligible_ids_query).fetchall()
            ]
            # Randomly select num_threads_per_category threads
            selected_ids = random.sample(
                eligible_ids, min(len(eligible_ids), num_threads_per_category)
            )
            log_with_resources(
                f"Selected {len(selected_ids)} threads from {source_tbl}."
            )

            all_ids_to_move.extend(selected_ids)
            all_ids_to_move = list(set(all_ids_to_move))
        except Exception as e:
            log_with_resources(
                f"Error processing {source_tbl}: {e}. Continuing with other tables."
            )

    # Collect all eligible IDs from all subreddit tables
    all_subreddit_ids = []
    for source_tbl in subreddit_tables:
        try:
            eligible_ids_query = f"""
                SELECT t1.comments_to_posts
                FROM {source_tbl} AS t1
                JOIN {table_to_filter} AS t2
                ON t1.comments_to_posts = t2.comments_to_posts
            """
            eligible_ids = [
                row[0] for row in con.execute(eligible_ids_query).fetchall()
            ]
            all_subreddit_ids.extend(eligible_ids)
            log_with_resources(
                f"Found {len(eligible_ids)} eligible threads from {source_tbl}."
            )
        except Exception as e:
            log_with_resources(
                f"Error processing {source_tbl}: {e}. Continuing with other tables."
            )

    # Remove duplicates from all subreddit IDs
    all_subreddit_ids = list(set(all_subreddit_ids))
    log_with_resources(
        f"Total unique threads across all subreddits: {len(all_subreddit_ids)}"
    )

    # Randomly select up to 100 threads from all subreddits combined
    subreddit_sample_size = min(100, len(all_subreddit_ids))
    if all_subreddit_ids:
        selected_subreddit_ids = random.sample(all_subreddit_ids, subreddit_sample_size)
        log_with_resources(
            f"Selected {len(selected_subreddit_ids)} threads from all subreddits combined."
        )
        all_ids_to_move.extend(selected_subreddit_ids)
        all_ids_to_move = list(set(all_ids_to_move))

    if all_ids_to_move:
        # Create a temporary table for IDs to move
        con.execute("CREATE TEMPORARY TABLE temp_ids_to_move (id VARCHAR);")
        con.executemany(
            "INSERT INTO temp_ids_to_move VALUES (?)", [(id,) for id in all_ids_to_move]
        )

        # Insert selected threads into testing_threads
        count_inserted_before = con.execute(
            f"SELECT COUNT(*) FROM {new_table};"
        ).fetchone()[0]
        con.execute(
            f"""
            INSERT INTO {new_table}
            SELECT t1.*
            FROM {table_to_filter} AS t1
            JOIN temp_ids_to_move AS t2 ON t1.comments_to_posts = t2.id;
        """
        )
        count_inserted_after = con.execute(
            f"SELECT COUNT(*) FROM {new_table};"
        ).fetchone()[0]
        log_with_resources(
            f"Inserted {count_inserted_after - count_inserted_before} threads into {new_table}."
        )

        # Delete selected threads from training_threads
        count_deleted_before = con.execute(
            f"SELECT COUNT(*) FROM {table_to_filter};"
        ).fetchone()[0]
        con.execute(
            f"""
            DELETE FROM {table_to_filter}
            WHERE comments_to_posts IN (SELECT id FROM temp_ids_to_move);
        """
        )
        count_deleted_after = con.execute(
            f"SELECT COUNT(*) FROM {table_to_filter};"
        ).fetchone()[0]
        log_with_resources(
            f"Deleted {count_deleted_before - count_deleted_after} threads from {table_to_filter}."
        )

        # Drop the temporary table
        con.execute("DROP TABLE temp_ids_to_move;")
    else:
        log_with_resources("No threads found to move to testing_threads.")
    log_with_resources(f"Finished creation of {new_table}.")


def filter_by_constructiveness(con, table_to_filter, new_table, jsonl_file):
    """
    Create a subset of constructive threads from table_to_filter and save to new_table by checking
    if the comments_to_posts have prediction == 1 in the jsonl_file.
    """
    import json

    log_with_resources(f"Starting constructiveness filtering from {jsonl_file}...")

    # Read the jsonl file and collect all sdids with prediction == 1
    constructive_sdids = set()

    try:
        with open(jsonl_file, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    # Check if this entry has prediction == 1 (constructive)
                    if data.get("prediction") == 1:
                        constructive_sdids.add(data.get("sdid"))
                except json.JSONDecodeError as e:
                    log_with_resources(f"Error parsing JSON on line {line_num}: {e}")
                    continue
    except FileNotFoundError:
        log_with_resources(f"Error: Could not find file {jsonl_file}")
        return
    except Exception as e:
        log_with_resources(f"Error reading file {jsonl_file}: {e}")
        return

    log_with_resources(
        f"Found {len(constructive_sdids)} constructive entries in {jsonl_file}"
    )

    if not constructive_sdids:
        log_with_resources("No constructive entries found. Creating empty table.")
        # Create empty table with same structure
        con.execute(
            f"CREATE OR REPLACE TABLE {new_table} AS SELECT * FROM {table_to_filter} WHERE FALSE"
        )
        return

    # Create a temporary table with the constructive sdids for efficient filtering
    con.execute("CREATE TEMPORARY TABLE temp_constructive_sdids (sdid VARCHAR)")

    # Insert constructive sdids in batches
    constructive_list = list(constructive_sdids)
    batch_size = 1000
    for i in range(0, len(constructive_list), batch_size):
        batch = constructive_list[i : i + batch_size]
        con.executemany(
            "INSERT INTO temp_constructive_sdids VALUES (?)",
            [(sdid,) for sdid in batch],
        )

    # Filter the table to include only constructive threads
    # The comments_to_posts column contains the ID that should match with sdid
    filter_query = f"""
    CREATE OR REPLACE TABLE {new_table} AS
    SELECT t.*
    FROM {table_to_filter} t
    JOIN temp_constructive_sdids c ON t.comments_to_posts = c.sdid
    """

    con.execute(filter_query)

    # Get count of filtered results
    count_result = con.execute(f"SELECT COUNT(*) FROM {new_table}").fetchone()[0]

    # Clean up temporary table
    con.execute("DROP TABLE temp_constructive_sdids")

    log_with_resources(
        f"Created {new_table} with {count_result} constructive threads from {table_to_filter}"
    )


def main():
    monitoring_active = True

    def continuous_resource_monitor(interval=1800):
        while monitoring_active:
            log_with_resources("Monitoring during execution")
            time.sleep(interval)

    # Start the background monitoring thread
    monitor_thread = threading.Thread(target=continuous_resource_monitor, args=(60,))
    monitor_thread.daemon = True  # will exit when main thread exits
    monitor_thread.start()

    db_path = "../data/database_subset10.db"
    con = duckdb.connect(db_path)
    log_with_resources("initial resources")
    con.execute("SET threads TO 20;")
    con.execute("PRAGMA verify_parallelism;")
    con.execute("PRAGMA memory_limit='30GB';")
    log_with_resources("threads set to 20")

    # Create initial tables
    add_initial_tables(con, "../data/posts.csv", "../data/comments.csv", limit=28000000)
    add_comments_to_comments_tables(con, "../data/comments.csv")
    for table in con.execute("SHOW TABLES").fetchdf()["name"]:
        print(f"Table: {table}")
        print(con.execute(f"SELECT COUNT(*) FROM {table}").fetchdf())
        print("\n")
    create_row_counts_table(con)
    cascading_comment_deletion(con, 75)
    create_row_counts_table(con)

    # Create threads tables
    create_lookup_table(con)
    create_threads_table(con=con, threads_table="all_threads")
    make_threads_unique(con, "threads")
    filter_threads(con, "threads", "threads", num_authors=None)
    filter_threads(
        con,
        "threads",
        "training_threads",
        num_authors=2,
        min_authors=True,
        check_english=True,
    )
    filter_by_constructiveness(
        con,
        "training_threads",
        "constructive_threads",
        "../training_data/reddit_train_annotated.jsonl",
    )

    threads_tables = ["threads", "training_threads", "constructive_threads"]
    for thread_table in threads_tables:
        # Create subsets with 2,3,4,5 authors
        for i in range(2, 6):
            filter_threads(
                con, thread_table, f"{thread_table}_{i}_authors", num_authors=i
            )

        # Create viral and non-viral subsets
        filter_by_score(con, thread_table)
        with open("../data/saved_stats.json", "r") as f:
            existing_data = json.load(f)
        distribution = existing_data[f"subreddit_distribution_{thread_table}"]
        subreddits = [
            key
            for key, value in sorted(
                distribution.items(), key=lambda x: x[1], reverse=True
            )[:5]
        ]
        # Create tables and stats for top 5 subreddits in each main table
        for subreddit in subreddits:
            create_subreddit_tables(con, subreddit, threads_table=thread_table)

    monitoring_active = False
    monitor_thread.join()
    log_with_resources("Script finished")
    con.commit()
    con.close()


if __name__ == "__main__":
    main()
