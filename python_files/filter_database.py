from stats import log_with_resources
import concurrent.futures
import duckdb


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


def filter_threads(con, table_to_filter, new_table, num_authors=None):
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
