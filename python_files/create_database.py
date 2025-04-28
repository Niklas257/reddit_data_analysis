import re
import pandas as pd
import gc
import os
import duckdb
from stats import log_with_resources
from concurrent.futures import ThreadPoolExecutor, as_completed


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
    log_with_resources("posts table created successfully.")


def add_comments_working_table(con, comments_file):
    # Create comments_working table
    batch_size = 1000000  # Adjust based on your needs

    # First create the target table structure
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


def add_initial_tables(con, posts_file, comments_file):
    # Create posts table
    query = f"""
    CREATE OR REPLACE TABLE posts AS
    SELECT id, title, selftext, subreddit, score, upvote_ratio, media, author
    FROM read_csv_auto('{posts_file}',
                    null_padding=True,
                    ignore_errors=True)
    LIMIT 2800000
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
        num = int(re.search(r"comments_to_comments_(\d+)", t).group(1))
        return (2, num)


# Create lookup table
def create_lookup_table_old(con):
    # Retrieve list of tables from the main schema.
    tables = con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
    ).fetchall()

    valid_tables = filter_valid_tables(tables)
    valid_tables.sort(key=sort_key)

    # Build subqueries for each descendant level that aggregate ids by post.
    # Note: valid_tables[0] is "posts", so we start from index 1.
    subqueries = []
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

    # Materialize each subquery into a temporary table.
    for table_name, subquery in subqueries:
        temp_sql = f"CREATE TEMP TABLE temp_{table_name} AS\n{subquery}"
        con.execute(temp_sql)
        con.commit()

    # Build the final lookup table by joining the temporary tables.
    final_sql = "CREATE TABLE lookup_table AS\nSELECT p.id as posts"
    for table_name, _ in subqueries:
        final_sql += f", temp_{table_name}.{table_name}"
    final_sql += "\nFROM posts p\n"
    for table_name, _ in subqueries:
        final_sql += (
            f"LEFT JOIN temp_{table_name} ON temp_{table_name}.post_id = p.id\n"
        )

    con.execute(final_sql)
    con.commit()
    log_with_resources("lookup_table created successfully.")

    # Clean up: drop the temporary tables as they are no longer needed.
    for table_name, _ in subqueries:
        drop_sql = f"DROP TABLE IF EXISTS temp_{table_name}"
        con.execute(drop_sql)
        con.commit()
        log_with_resources(f"Temp table {table_name} dropped")


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


def process_depth_query(query, db_path):
    """
    Open a new connection, execute the provided query,
    fetch all results, and return them.
    """
    con_worker = duckdb.connect(db_path)
    try:
        results = con_worker.execute(query).fetchall()
    except Exception as e:
        print(f"Error in query execution: {e}")
        results = []
    finally:
        con_worker.close()
    return results


def create_threads_table_parallel(con, threads_table, max_workers=None, batch_size=5):
    """Create threads table with parallel query execution with safeguards

    Args:
        con: DuckDB connection
        threads_table: Name of the output table
        max_workers: Number of parallel workers
        batch_size: Process depths in smaller batches to control memory usage
    """
    # Use fewer threads to avoid overwhelming the system
    cpu_count = os.cpu_count()
    if max_workers is None:
        # More conservative initial setting
        max_workers = min(24, cpu_count // 4)

    # Configure DuckDB with fewer threads initially
    con.execute(f"SET threads TO {cpu_count // 2};")
    log_with_resources(f"DuckDB thread count set to {cpu_count // 2}")

    # Get column information
    columns = con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
    ).fetchall()
    columns = filter_valid_tables(columns)
    columns.sort(key=sort_key)

    # Create the target table
    create_table_sql = f"""
    CREATE OR REPLACE TABLE {threads_table} (
        {', '.join(f'{col} VARCHAR' for col in columns)}
    )"""
    con.execute(create_table_sql)
    log_with_resources(f"Created empty table {threads_table}")

    # Use thread-local storage for connection management
    import threading

    threading.local()

    # Function that processes a depth with proper connection management
    def process_depth(depth):
        try:
            # Determine the starting table name based on depth
            if depth == 0:
                starting_table = "posts"
            elif depth == 1:
                starting_table = "comments_to_posts"
            else:
                starting_table = f"comments_to_comments_{depth - 1}"

            # Generate query as before...
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

            select_parts = []
            for idx, col in enumerate(columns):
                if idx > depth:
                    select_parts.append(f"NULL AS {col}")
                else:
                    select_parts.append(f"t{idx}.id AS {col}")

            # Construct the SQL query for this depth
            query = f"""
            INSERT INTO {threads_table}
            SELECT {', '.join(select_parts)}
            FROM {starting_table} AS t{depth}
            {' '.join(join_clauses)}
            """

            # Acquire a lock to use the shared connection safely
            # This is less parallel but avoids connection conflicts
            with connection_lock:
                con.execute(query)

            log_with_resources(f"Completed processing depth {depth}")
            return f"Successfully processed depth {depth}"
        except Exception as e:
            log_with_resources(f"Error processing depth {depth}: {str(e)}")
            raise

    # Create a lock for connection access
    connection_lock = threading.Lock()

    # Process in smaller batches to control memory usage
    all_depths = list(range(len(columns) - 1, -1, -1))

    log_with_resources(
        f"Starting parallel execution with {max_workers} workers in batches of {batch_size}"
    )

    # Process in smaller batches to control memory usage
    for i in range(0, len(all_depths), batch_size):
        batch_depths = all_depths[i : i + batch_size]
        log_with_resources(
            f"Processing batch {i//batch_size + 1}: depths {batch_depths}"
        )

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit batch tasks
            future_to_depth = {
                executor.submit(process_depth, depth): depth for depth in batch_depths
            }

            # Process results as they complete
            for future in as_completed(future_to_depth):
                depth = future_to_depth[future]
                try:
                    result = future.result()
                    log_with_resources(result)
                except Exception as e:
                    log_with_resources(
                        f"Depth {depth} generated an exception: {str(e)}"
                    )

        # Force garbage collection between batches
        import gc

        gc.collect()

    # Get statistics for the created table
    table_stats = con.execute(f"SELECT COUNT(*) FROM {threads_table}").fetchone()[0]
    log_with_resources(f"Created {threads_table} with {table_stats} rows")

    # Run ANALYZE to update statistics
    con.execute(f"ANALYZE {threads_table}")
