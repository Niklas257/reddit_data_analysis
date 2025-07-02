from stats import log_with_resources
from langdetect import detect
import json
import random


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
        CREATE TABLE threads_viral AS
        SELECT *
        FROM {table_to_filter}
        WHERE posts IN (SELECT id FROM posts WHERE score >= 1000)
        """
    )
    log_with_resources("Created threads_viral table")
    con.execute(
        f"""
        CREATE TABLE threads_non_viral AS
        SELECT *
        FROM {table_to_filter}
        WHERE posts IN (SELECT id FROM posts WHERE score < 1000)
        """
    )
    log_with_resources("Created threads_non_viral table")


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

    with open("../data/saved_stats.json", "r") as f:
        existing_data = json.load(f)
    distribution = existing_data["subreddit_distribution_threads"]
    for key, value in sorted(distribution.items(), key=lambda x: x[1], reverse=True)[
        :5
    ]:
        source_tables.append(f"{key}_threads")
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
