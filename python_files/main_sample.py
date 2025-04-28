import duckdb
import json
import threading
import time
from create_database import (
    add_initial_tables,
    add_comments_to_comments_tables_old,
    cascading_comment_deletion,
    create_lookup_table,
    create_subreddit_tables,
    create_threads_table,
)
from stats import (
    create_row_counts_table,
    get_depth_distribution,
    get_number_of_threads,
    get_thread_score_distribution,
    get_subreddit_distribution,
    table_stats,
    calculate_weighted_average,
    get_thread_lengths,
    get_author_distribution,
    log_with_resources,
)

from filter_database import make_threads_unique, filter_threads

monitoring_active = True
max_workers = 90
num_partitions = 120


def continuous_resource_monitor(interval=1800):
    while monitoring_active:
        log_with_resources("Monitoring during execution")
        time.sleep(interval)


# Start the background monitoring thread
monitor_thread = threading.Thread(target=continuous_resource_monitor, args=(300,))
monitor_thread.daemon = True  # will exit when main thread exits
monitor_thread.start()

db_path = "../data/database.db"
con = duckdb.connect(db_path)
log_with_resources("initial resources")
con.execute("SET threads TO 96;")
con.execute("PRAGMA verify_parallelism;")
con.execute("PRAGMA memory_limit='500GB';")
log_with_resources("threads set to 96")
"""
add_initial_tables(con, "../data/posts.csv", "../data/comments.csv")
add_comments_to_comments_tables_old(con, "../data/comments.csv")"""
for table in con.execute("SHOW TABLES").fetchdf()["name"]:
    print(f"Table: {table}")
    print(con.execute(f"SELECT COUNT(*) FROM {table}").fetchdf())
    print("\n")

create_row_counts_table(con)
df = con.execute("SELECT * FROM row_counts").fetchdf()

df = df.sort_values(by="row_count", ascending=False)
# Row count of comments to posts
total_rows = df.loc[df["table_name"] == "comments_to_posts", "row_count"].values[0]
print(f"Total number of threads: {total_rows}")
# Find the table after which there are only 5% of the rows left
for i in range(1, len(df) - 1):
    if (
        df.loc[df["table_name"] == f"comments_to_comments_{i}", "row_count"].values[0]
        / total_rows
        < 0.05
    ):
        # Get the last character of the table name
        table_number = i
        break
print(
    f"Table after which there are only 5% of the rows left: {f"comments_to_comments_{table_number}"}"
)
# Drop the tables after this table
i = table_number + 1
while True:
    try:
        con.execute(f"DROP TABLE comments_to_comments_{i}")
        i += 1
    except Exception as e:
        print(f"comments_to_comments_{i} does not exist")
        break

cascading_comment_deletion(con, table_number)
create_row_counts_table(con)

create_lookup_table(con)
create_threads_table(con=con, threads_table="all_threads")

table_stats("lookup_table", con)
calculate_weighted_average("thread_lengths_lookup_table")
calculate_weighted_average("thread_widths_lookup_table")
calculate_weighted_average("all_widths_lookup_table")

make_threads_unique(con, "threads")
filter_threads(con, "threads", 3)

get_depth_distribution("threads", con)
get_depth_distribution("filtered_threads", con)
get_thread_lengths("threads", con)
get_thread_lengths("filtered_threads", con)
get_number_of_threads("threads", con)
get_number_of_threads("filtered_threads", con)
get_thread_score_distribution("threads", con)
get_thread_score_distribution("filtered_threads", con)
get_subreddit_distribution("threads", con)
get_subreddit_distribution("filtered_threads", con)
get_author_distribution("threads", con)
get_author_distribution("filtered_threads", con)

with open("../data/saved_stats.json", "r") as f:
    existing_data = json.load(f)
distribution = existing_data["subreddit_distribution_threads"]
subreddits = [
    key
    for key, value in sorted(distribution.items(), key=lambda x: x[1], reverse=True)[:5]
]
for subreddit in subreddits:
    create_subreddit_tables(con, subreddit)
    table_stats(f"{subreddit}_lookup", con)
    calculate_weighted_average(f"thread_lengths_{subreddit}_lookup")
    calculate_weighted_average(f"thread_widths_{subreddit}_lookup")
    calculate_weighted_average(f"all_widths_{subreddit}_lookup")
    get_depth_distribution(f"{subreddit}_threads", con)
    get_depth_distribution(f"filtered_{subreddit}_threads", con)
    get_thread_lengths(f"{subreddit}_threads", con)
    get_thread_lengths(f"filtered_{subreddit}_threads", con)
    get_number_of_threads(f"{subreddit}_threads", con)
    get_number_of_threads(f"filtered_{subreddit}_threads", con)
    get_thread_score_distribution(f"{subreddit}_threads", con)
    get_thread_score_distribution(f"filtered_{subreddit}_threads", con)
    get_author_distribution(f"{subreddit}_threads", con)
    get_author_distribution(f"filtered_{subreddit}_threads", con)


monitoring_active = False
monitor_thread.join()  # optional, if you want to ensure it has stopped before exiting
log_with_resources("Script finished")
con.commit()
con.close()
