import pandas as pd
import random
from textwrap import fill
from tqdm import tqdm
import json
from create_database import log_with_resources


def get_random_thread_details(table, con, seed, verbose=False):

    # Get the count of rows in table
    count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    if count == 0:
        print(f"No threads found in {table} table.")
        return None

    # Select a random row from table
    random.seed(seed)
    random_offset = random.randint(0, count - 1)
    random_thread = con.execute(
        f"SELECT * FROM {table} ORDER BY comments_to_posts OFFSET {random_offset} LIMIT 1"
    ).fetchdf()
    if verbose:
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


def create_subset_tables(con, table):
    for i in range(3):
        subset_table = f"{table}_subset_{i+1}"
        # Create an empty subset table with the same schema as 'threads'
        # This uses a trick: select no rows from the source table.
        # Check if {table} exists in the database
        if not con.execute(
            f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'"
        ).fetchone():
            print(f"Table {table} does not exist.")
            return
        con.execute(
            f"CREATE OR REPLACE TABLE {subset_table} AS SELECT * FROM {table} WHERE 1=0"
        )

        # Insert 3 rows into each subset table.
        for j in range(3):
            seed = i * 100 + j  # Fixed, unique seed for each insertion.
            thread = get_random_thread_details(table, con, seed)

            # Assume 'thread' is a DataFrame with one row.
            # Extract the column names and values.
            row = thread.iloc[0]
            cols = thread.columns.tolist()

            # Quote column names; this can help if any names conflict with SQL keywords.
            col_list = ", ".join([f'"{col}"' for col in cols])
            # Create a parameter placeholder for each column.
            placeholders = ", ".join(["?" for _ in cols])
            sql = f"INSERT INTO {subset_table} ({col_list}) VALUES ({placeholders})"
            values = tuple(row)

            con.execute(sql, values)


def generate_jsonl_from_threads(
    con, training_threads_table, output_filepath, testing=False
):
    """
    Generates a .jsonl file from the specified training threads table.

    Each line in the .jsonl file will be a JSON object with:
    - "sdid": The ID from the 'comments_to_posts' column of the thread.
    - "text": A concatenated string of author-prefixed content from the thread,
              starting with the 'posts' content, then subsequent comments.

    Args:
        con: A DuckDB database connection object.
        training_threads_table (str): The name of the table containing the filtered threads (e.g., "training_threads").
        output_filepath (str): The path to the output .jsonl file.
    """

    # 1. Get column names and their order from the training_threads_table
    thread_column_info = con.execute(
        f"""
        SELECT column_name, ordinal_position
        FROM information_schema.columns
        WHERE table_name = '{training_threads_table}'
        ORDER BY ordinal_position
        """
    ).fetchall()

    thread_column_names = [col[0] for col in thread_column_info]

    # Find the index of the 'comments_to_posts' column
    try:
        comments_to_posts_idx = thread_column_names.index("comments_to_posts")
    except ValueError:
        raise ValueError(
            f"Column 'comments_to_posts' not found in table '{training_threads_table}'. "
            "Please ensure the table has this column for sdid."
        )

    # Pre-fetch all necessary content and author data into a single lookup dictionary
    id_to_info = {}  # Stores {'id': {'author': '...', 'content': '...'}}

    # Fetch posts data
    posts_data = con.execute(
        "SELECT id, author, title, selftext, subreddit FROM posts"
    ).fetchall()
    for post_id, author, title, selftext, subreddit in posts_data:
        combined_content = f"{title or ''} {selftext or ''}".strip()
        id_to_info[post_id] = {
            "author": author,
            "content": combined_content,
            "subreddit": subreddit,
        }

    # Fetch comments data for all relevant comment tables
    comment_tables = con.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main'
        AND (table_name LIKE 'comments_to_posts%' OR table_name LIKE 'comments_to_comments_%')
        """
    ).fetchall()
    comment_tables = [tbl[0] for tbl in comment_tables]

    for table_name in comment_tables:
        col_check = con.execute(f"PRAGMA table_info('{table_name}');").fetchall()
        if any(c[1] == "body" for c in col_check):
            comments_data = con.execute(
                f"SELECT id, author, body FROM {table_name}"
            ).fetchall()
            for comment_id, author, body in comments_data:
                id_to_info[comment_id] = {"author": author, "content": body or ""}

    # Open the output .jsonl file
    with open(output_filepath, "w", encoding="utf-8") as f:
        # Fetch all rows from the training_threads table
        threads_data = con.execute(f"SELECT * FROM {training_threads_table}").fetchall()

        # Process each thread (row)
        for thread_row in threads_data:
            # Get the sdid from the comments_to_posts column
            sdid = thread_row[comments_to_posts_idx]

            # Initialize author mapping and text parts for the current thread
            current_thread_authors = {}
            author_counter = 0
            text_parts = []

            # Process the initial post (which is always the first column, index 0)
            post_id = thread_row[0]
            if post_id is not None:
                post_info = id_to_info.get(post_id)
                if post_info:
                    author = post_info["author"]
                    content = post_info["content"]

                    # Assign [author0] to the author of the initial post
                    if author not in current_thread_authors:
                        current_thread_authors[author] = f"[author{author_counter}]"
                        author_counter += 1

                    text_parts.append(f"{current_thread_authors[author]} {content}")
                else:
                    log_with_resources(
                        f"Warning: Post ID {post_id} from thread {sdid} not found in pre-fetched content data."
                    )

            # Process subsequent comments (from index 1 onwards in the thread_row)
            # This covers comments_to_posts, comments_to_comments_1, etc.
            for id_in_thread_row in thread_row[1:]:  # Start from the second column
                if id_in_thread_row is None:
                    continue  # Skip empty slots in the thread structure

                item_info = id_to_info.get(id_in_thread_row)

                if item_info:
                    author = item_info["author"]
                    content = item_info["content"]

                    # Assign anonymized author ID if not already mapped for this thread
                    if author not in current_thread_authors:
                        current_thread_authors[author] = f"[author{author_counter}]"
                        author_counter += 1

                    anonymized_author = current_thread_authors[author]
                    text_parts.append(f"{anonymized_author} {content}")
                else:
                    log_with_resources(
                        f"Warning: Comment ID {id_in_thread_row} from thread {sdid} not found in pre-fetched content data."
                    )

            # Join all text parts to form the final thread text
            full_thread_text = " ".join(text_parts)

            # Create the JSON object for the current thread
            json_object = {"sdid": sdid, "text": full_thread_text}

            if testing:
                subreddit = id_to_info.get(post_id, {}).get("subreddit")
                json_object["subreddit"] = subreddit if subreddit else "unknown"

            # Write the JSON object as a line in the .jsonl file
            f.write(json.dumps(json_object, ensure_ascii=False) + "\n")

    log_with_resources(
        f"Successfully generated {output_filepath} with data from {training_threads_table}."
    )
