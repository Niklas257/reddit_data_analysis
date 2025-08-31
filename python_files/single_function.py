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
    calculate_variance,
    get_thread_lengths,
    get_author_distribution,
    log_with_resources,
)

from filter_database import (
    make_threads_unique,
    filter_threads,
    filter_by_score,
    filter_by_constructiveness,
)

monitoring_active = True
max_workers = 90
num_partitions = 120


def continuous_resource_monitor(interval=1800):
    while monitoring_active:
        log_with_resources("Monitoring during execution")
        time.sleep(interval)


# Start the background monitoring thread
monitor_thread = threading.Thread(target=continuous_resource_monitor, args=(30,))
monitor_thread.daemon = True  # will exit when main thread exits
monitor_thread.start()

db_path = "../data/database_subset10.db"
con = duckdb.connect(db_path)
log_with_resources("initial resources")
con.execute("SET threads TO 20;")
con.execute("PRAGMA verify_parallelism;")
con.execute("PRAGMA memory_limit='30GB';")
log_with_resources("threads set to 20")
"""for table in con.execute("SHOW TABLES").fetchdf()["name"]:
    print(f"Table: {table}")
    print(con.execute(f"SELECT COUNT(*) FROM {table}").fetchdf())
    print("\n")

filter_by_constructiveness(
    con,
    "training_threads",
    "constructive_threads",
    "../training_data/reddit_train_annotated.jsonl",
)"""

"""
get_depth_distribution("training_threads", con)
get_thread_lengths("training_threads", con)
get_number_of_threads("training_threads", con)
get_thread_score_distribution("training_threads", con)
get_subreddit_distribution("training_threads", con)
get_author_distribution("training_threads", con)

calculate_weighted_average("depth_distribution_training_threads")
calculate_weighted_average("author_distribution_training_threads")
calculate_weighted_average("thread_score_distribution_training_threads")
calculate_weighted_average("thread_lengths_training_threads")
calculate_variance("depth_distribution_training_threads")
calculate_variance("author_distribution_training_threads")
calculate_variance("thread_score_distribution_training_threads")
calculate_variance("thread_lengths_training_threads")

# Create subsets with 2,3,4,5 authors

filter_threads(
    con,
    "training_threads",
    "training_threads_2_authors",
    num_authors=2,
)
filter_threads(
    con,
    "training_threads",
    "training_threads_3_authors",
    num_authors=3,
)
filter_threads(
    con,
    "training_threads",
    "training_threads_4_authors",
    num_authors=4,
)
filter_threads(
    con,
    "training_threads",
    "training_threads_5_authors",
    num_authors=5,
)

for i in range(2, 6):
    get_depth_distribution(f"training_threads_{i}_authors", con)
    get_thread_lengths(f"training_threads_{i}_authors", con)
    get_number_of_threads(f"training_threads_{i}_authors", con)
    get_thread_score_distribution(f"training_threads_{i}_authors", con)
    get_subreddit_distribution(f"training_threads_{i}_authors", con)
    calculate_weighted_average(f"depth_distribution_training_threads_{i}_authors")
    calculate_weighted_average(
        f"thread_score_distribution_training_threads_{i}_authors"
    )
    calculate_weighted_average(f"thread_lengths_training_threads_{i}_authors")
    calculate_variance(f"depth_distribution_training_threads_{i}_authors")
    calculate_variance(f"thread_score_distribution_training_threads_{i}_authors")
    calculate_variance(f"thread_lengths_training_threads_{i}_authors")
filter_by_score(con, "training_threads")
for table in [
    "training_threads_viral",
    "training_threads_non_viral",
]:
    get_depth_distribution(table, con)
    get_thread_lengths(table, con)
    get_number_of_threads(table, con)
    get_thread_score_distribution(table, con)
    get_subreddit_distribution(table, con)
    get_author_distribution(table, con)
    calculate_weighted_average(f"depth_distribution_{table}")
    calculate_weighted_average(f"author_distribution_{table}")
    calculate_weighted_average(f"thread_score_distribution_{table}")
    calculate_weighted_average(f"thread_lengths_{table}")
    calculate_variance(f"depth_distribution_{table}")
    calculate_variance(f"author_distribution_{table}")
    calculate_variance(f"thread_score_distribution_{table}")
    calculate_variance(f"thread_lengths_{table}")
"""
subreddits = [
    "AskReddit",
    "memes",
    "distantsocializing",
    "ACTrade",
    "RedditSessions",
]
for subreddit in subreddits:
    create_subreddit_tables(con, subreddit, threads_table="training_threads")
    get_depth_distribution(f"{subreddit}_training_threads", con)
    get_thread_lengths(f"{subreddit}_training_threads", con)
    get_number_of_threads(f"{subreddit}_training_threads", con)
    get_thread_score_distribution(f"{subreddit}_training_threads", con)
    get_author_distribution(f"{subreddit}_training_threads", con)
    calculate_weighted_average(f"depth_distribution_{subreddit}_training_threads")
    calculate_weighted_average(f"author_distribution_{subreddit}_training_threads")
    calculate_weighted_average(
        f"thread_score_distribution_{subreddit}_training_threads"
    )
    calculate_weighted_average(f"thread_lengths_{subreddit}_training_threads")
    calculate_variance(f"depth_distribution_{subreddit}_training_threads")
    calculate_variance(f"author_distribution_{subreddit}_training_threads")
    calculate_variance(f"thread_score_distribution_{subreddit}_training_threads")
    calculate_variance(f"thread_lengths_{subreddit}_training_threads")


monitoring_active = False
monitor_thread.join()
log_with_resources("Script finished")
con.commit()
con.close()
