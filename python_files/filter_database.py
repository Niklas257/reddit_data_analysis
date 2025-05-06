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


def process_partition(
    partition_id, num_partitions, non_null_counts_str, columns_str, db_path
):
    """
    Process one partition: runs the windowing query on rows for which
    MOD(HASH(comments_to_posts), num_partitions) equals partition_id.
    Returns the results as a list of tuples.
    """
    # Each process uses its own connection.
    con_local = duckdb.connect(db_path)
    query = f"""
    WITH ranked_threads AS (
      SELECT
        *,
        (CASE WHEN posts IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN comments_to_posts IS NOT NULL THEN 1 ELSE 0 END +
         {non_null_counts_str}
        ) AS thread_length,
        ROW_NUMBER() OVER (
          PARTITION BY comments_to_posts
          ORDER BY thread_length DESC, RANDOM()
        ) AS rn
      FROM all_threads
      WHERE comments_to_posts IS NOT NULL
        AND MOD(HASH(comments_to_posts), {num_partitions}) = {partition_id}
    )
    SELECT {columns_str}
    FROM ranked_threads
    WHERE rn = 1
    ORDER BY comments_to_posts;
    """
    try:
        results = con_local.execute(query).fetchall()
    except Exception as e:
        print(f"Error in partition {partition_id}: {e}")
        results = []
    con_local.close()
    return results


def make_threads_unique_parallel(
    con,
    filtered_table,
    db_path,
    num_partitions=15,
    max_workers=15,
):
    """
    Optimized version that partitions the workload by comments_to_posts and
    processes each partition in parallel.

    Parameters:
      con            : Main DuckDB connection.
      filtered_table : Name of the output (filtered) table.
      db_path        : File path to the DuckDB database.
      num_partitions : Number of partitions to split the workload.
      max_workers    : Number of parallel processes to use.
    """
    # 1. Retrieve the schema from the lookup_table.
    cursor = con.execute("PRAGMA table_info('lookup_table')")
    columns = [row[1] for row in cursor.fetchall()]
    print(f"Columns in lookup_table: {columns}")
    columns_str = ", ".join(columns)

    # 2. Build the dynamic SQL snippet for counting non-NULL columns.
    # We assume columns related to "comments_to_comments_*" start at index 1 and go to len(columns)-1.
    non_null_counts = [
        f"CASE WHEN comments_to_comments_{i} IS NOT NULL THEN 1 ELSE 0 END"
        for i in range(1, len(columns) - 1)
    ]
    non_null_counts_str = " + ".join(non_null_counts)

    # Log the constructed query parts.
    log_with_resources("Constructed dynamic SQL parts for non-NULL counts and columns.")

    # 3. Partition the work: Process each partition in parallel.
    all_results = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submit one future per partition.
        futures = [
            executor.submit(
                process_partition,
                partition_id,
                num_partitions,
                non_null_counts_str,
                columns_str,
                db_path,
            )
            for partition_id in range(num_partitions)
        ]
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                all_results.extend(result)
            except Exception as e:
                print(f"Error during parallel execution: {e}")

    log_with_resources(
        f"Parallel query execution complete. Retrieved {len(all_results)} rows."
    )

    # 4. Create (or replace) the destination table with the proper schema.
    create_table_sql = f"""
    CREATE OR REPLACE TABLE {filtered_table} (
        {', '.join(f'{col} VARCHAR' for col in columns)}
    )
    """
    con.execute(create_table_sql)
    con.commit()
    log_with_resources(f"Created/replaced table {filtered_table}.")

    # 5. Insert all results into the filtered table.
    # DuckDB uses '?' as the placeholder for executemany.
    placeholders = ", ".join(["?"] * len(columns))
    insert_sql = f"INSERT INTO {filtered_table} VALUES ({placeholders})"
    con.executemany(insert_sql, all_results)
    con.commit()
    log_with_resources(f"Inserted unioned results into {filtered_table}.")


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


def process_rows_chunk(chunk, id_to_author, id_to_content, num_authors):
    """
    Process a list of rows (a chunk) and return only those rows that satisfy:
      - No ID in the row has deleted/removed content.
      - The row contains exactly num_authors unique authors,
        each of whom appears at least twice.
    """
    valid_rows = []
    for row in chunk:
        # Skip rows if any ID has deleted or removed content
        if any(
            id_value is not None and id_to_content.get(id_value, False)
            for id_value in row
        ):
            continue

        # Count appearances of each author (skip first column 'posts')
        authors = {}
        for id_value in row[1:]:
            if id_value is None:
                continue
            author = id_to_author.get(id_value)
            if author:
                authors[author] = authors.get(author, 0) + 1

        # Check if the row meets the criteria.
        if len(authors) == num_authors and all(
            count >= 2 for count in authors.values()
        ):
            valid_rows.append(row)
    return valid_rows


# ---------------------------
# Revised Filter Function (Optimized for Parallelism)
# ---------------------------
def filter_threads_parallel(
    con, table_to_filter, num_authors, chunk_size=10000, max_workers=15
):
    """
    Creates a new table 'filtered_threads' by filtering rows from the provided table.
    The filtering criteria is based on prefetching ID-to-author mapping and then,
    in parallel, processing the rows to keep only threads that:
      - Have no [deleted] or [removed] content.
      - Have exactly num_authors unique authors with each author appearing
        at least 2 times.

    Parameters:
      con          : Open DuckDB connection.
      table_to_filter : Name of the table to be filtered.
      num_authors  : The number of distinct authors that must appear.
      chunk_size   : How many rows to process in each chunk.
      max_workers  : Number of worker processes to use for filtering.
    """
    # 1. Create a new (empty) table for filtered results.
    con.execute(
        f"""
        CREATE OR REPLACE TABLE filtered_threads AS
        SELECT * FROM {table_to_filter} WHERE FALSE
        """
    )
    con.commit()
    log_with_resources("Created empty filtered_threads table.")

    # 2. Retrieve column names (excluding 'posts' if desired)
    columns = con.execute(
        f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{table_to_filter}'
          AND column_name != 'posts'
        """
    ).fetchall()
    # Assuming your table has 'posts' as the first column and others following:
    # We preserve the full order of columns as in the original table.
    columns = [
        col[1]
        for col in con.execute(f"PRAGMA table_info({table_to_filter})").fetchall()
    ]

    # 3. Prefetch ID-to-author and ID-to-content (deleted/removed) mappings.
    id_to_author = {}
    id_to_content = {}  # True if content is [deleted] or [removed]

    # Fetch posts data
    posts_data = con.execute("SELECT id, author, title, selftext FROM posts").fetchall()
    for id_val, author, title, selftext in posts_data:
        id_to_author[id_val] = author
        # Mark as deleted/removed if any field indicates so.
        id_to_content[id_val] = (
            author in ("[deleted]", "[removed]")
            or title in ("[deleted]", "[removed]")
            or selftext in ("[deleted]", "[removed]")
        )

    # Fetch comments data for each comment table (using the column names, excluding 'posts')
    for table in columns:
        if table == "posts":
            continue
        comments_data = con.execute(
            f"SELECT id, author, body FROM '{table}'"
        ).fetchall()
        for id_val, author, body in comments_data:
            id_to_author[id_val] = author
            id_to_content[id_val] = author in ("[deleted]", "[removed]") or body in (
                "[deleted]",
                "[removed]",
            )

    log_with_resources("Prefetched ID-to-author and ID-to-content mappings.")

    # 4. Fetch all rows from the threads table.
    rows = con.execute(f"SELECT * FROM {table_to_filter}").fetchall()
    log_with_resources(f"Fetched {len(rows)} rows from {table_to_filter}.")

    # 5. Partition the rows into chunks.
    chunks = [rows[i : i + chunk_size] for i in range(0, len(rows), chunk_size)]
    log_with_resources(f"Partitioned rows into {len(chunks)} chunks.")

    # 6. Process the chunks in parallel.
    valid_rows = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submit each chunk to be processed.
        futures = [
            executor.submit(
                process_rows_chunk, chunk, id_to_author, id_to_content, num_authors
            )
            for chunk in chunks
        ]
        # As each future completes, extend our valid_rows list.
        for future in concurrent.futures.as_completed(futures):
            try:
                valid_rows.extend(future.result())
            except Exception as e:
                print(f"Error processing chunk: {e}")
    log_with_resources(f"Filtered rows: {len(valid_rows)} valid threads remain.")

    # 7. Batch insert valid rows into the filtered_threads table.
    # Build placeholders for all columns (assuming the full row matches table schema).
    placeholders = ", ".join(["?"] * len(columns))
    insert_sql = f"INSERT INTO filtered_threads VALUES ({placeholders})"
    # Insert the valid rows into the filtered_threads table if any exist.
    if valid_rows:
        con.executemany(insert_sql, valid_rows)
        con.commit()
    log_with_resources(f"Inserted {len(valid_rows)} valid rows into filtered_threads.")


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
