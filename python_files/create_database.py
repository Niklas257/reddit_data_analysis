# Add initial tables to the database
import re


def add_initial_tables(con, posts_file, comments_file):
    # Create posts table
    query = f"""
    CREATE TABLE posts AS
    SELECT id, title, selftext, subreddit, score, upvote_ratio, media, author
    FROM read_csv_auto('{posts_file}',
                    null_padding=True,
                    ignore_errors=True);
    """
    con.execute(query)

    # Create comments_to_posts table
    query = f"""
    CREATE TABLE comments_to_posts AS
    SELECT id, body, score, author,
    SUBSTRING(c.parent_id, 4) AS parent_id,
    FROM read_csv_auto('{comments_file}',
                    null_padding=True,
                    ignore_errors=True) AS c
    WHERE SUBSTRING(c.parent_id, 4) IN (
    SELECT id FROM posts
    )
    """
    con.execute(query)

    # Create comments_to_comments_1 table
    query = f"""
    CREATE TABLE comments_to_comments_1 AS
    SELECT id, body, score, author,
    SUBSTRING(c.parent_id, 4) AS parent_id,
    FROM read_csv_auto('{comments_file}',
                    null_padding=True,
                    ignore_errors=True) AS c
    WHERE SUBSTRING(c.parent_id, 4) IN (
    SELECT id FROM comments_to_posts
    )
    """
    con.execute(query)


# Add comments_to_comments tables
def add_comments_to_comments_tables(con, comments_file):
    current_level = 1
    rows_found = 1  # Initialize with a non-zero value to enter the loop

    while rows_found > 0:
        next_level = current_level + 1

        # Query to find comments referencing the current level
        query = f"""
        SELECT id, body, score, author,
        SUBSTRING(c.parent_id, 4) AS parent_id,
        FROM read_csv_auto('{comments_file}',
                        null_padding=True,
                        ignore_errors=True) AS c
        WHERE SUBSTRING(c.parent_id, 4) IN (
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


# Create lookup table
def create_lookup_table(con):
    tables = con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
    ).fetchall()

    valid_tables = filter_valid_tables(tables)

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


def create_threads_table(con):
    cursor = con.execute("PRAGMA table_info('lookup_table')")
    columns = [row[1] for row in cursor.fetchall()]
    create_table_sql = f"""
    CREATE OR REPLACE TABLE threads (
        {', '.join(f'{col} VARCHAR' for col in columns)}
    )"""
    con.execute(create_table_sql)

    queries = []
    for depth in range(len(columns), 0, -1):
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
        final_query = "INSERT INTO threads\n" + "\nUNION ALL\n".join(queries)
        con.execute(final_query)
