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
    calculate_variance,
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
monitor_thread = threading.Thread(target=continuous_resource_monitor, args=(30,))
monitor_thread.daemon = True  # will exit when main thread exits
monitor_thread.start()

db_path = "../data/database.db"
# con = duckdb.connect(db_path)
log_with_resources("initial resources")
# con.execute("SET threads TO 96;")
# con.execute("PRAGMA verify_parallelism;")
# con.execute("PRAGMA memory_limit='500GB';")
log_with_resources("threads set to 96")

calculate_variance("depth_distribution_threads")
calculate_variance("author_distribution_threads")
calculate_variance("thread_score_distribution_threads")
calculate_variance("thread_lengths_threads")

for i in range(2, 6):
    calculate_variance(f"depth_distribution_threads_{i}_authors")
    calculate_variance(f"thread_score_distribution_threads_{i}_authors")
    calculate_variance(f"thread_lengths_threads_{i}_authors")

for table in ["threads_viral", "threads_non_viral"]:
    calculate_variance(f"depth_distribution_{table}")
    calculate_variance(f"author_distribution_{table}")
    calculate_variance(f"thread_score_distribution_{table}")
    calculate_variance(f"thread_lengths_{table}")

with open("../data/saved_stats.json", "r") as f:
    existing_data = json.load(f)
distribution = existing_data["subreddit_distribution_threads"]
subreddits = [
    key
    for key, value in sorted(distribution.items(), key=lambda x: x[1], reverse=True)[:5]
]
for subreddit in subreddits:
    calculate_variance(f"thread_lengths_{subreddit}_lookup")
    calculate_variance(f"depth_distribution_{subreddit}_threads")
    calculate_variance(f"author_distribution_{subreddit}_threads")
    calculate_variance(f"thread_score_distribution_{subreddit}_threads")
    calculate_variance(f"thread_lengths_{subreddit}_threads")


monitoring_active = False
monitor_thread.join()  # optional, if you want to ensure it has stopped before exiting
log_with_resources("Script finished")
# con.commit()
# con.close()
