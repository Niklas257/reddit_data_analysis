import duckdb
import json
import threading
import time
from create_database import (
    create_lookup_table,
    create_subreddit_tables,
    create_threads_table_parallel,
)
from stats import (
    create_row_counts_table,
    get_depth_distribution,
    get_number_of_threads,
    get_thread_score_distribution_parallel,
    get_subreddit_distribution,
    table_stats_parallel,
    calculate_weighted_average,
    get_thread_lengths,
    get_author_distribution_parallel,
    log_with_resources,
)

from filter_database import make_threads_unique_parallel, filter_threads_parallel

monitoring_active = True


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
con.execute("SET threads TO 120;")
con.execute("PRAGMA verify_parallelism;")
con.execute("PRAGMA memory_limit='128GB';")
log_with_resources("threads set to 120")

create_row_counts_table(con)

create_lookup_table(con)

table_stats_parallel("lookup_table", con, num_workers=90, chunk_size=10000)
calculate_weighted_average("thread_lengths_lookup_table")
calculate_weighted_average("thread_widths_lookup_table")
calculate_weighted_average("all_widths_lookup_table")

create_threads_table_parallel(con, "all_threads", db_path, max_workers=90)
make_threads_unique_parallel(
    con, "threads", db_path, num_partitions=120, max_workers=90
)
filter_threads_parallel(con, "threads", 3, chunk_size=10000, max_workers=90)

get_depth_distribution("threads", con)
get_depth_distribution("filtered_threads", con)
get_thread_lengths("threads", con)
get_thread_lengths("filtered_threads", con)
get_number_of_threads("threads", con)
get_number_of_threads("filtered_threads", con)
get_thread_score_distribution_parallel(
    "threads", con, db_path, num_partitions=120, max_workers=90
)
get_thread_score_distribution_parallel(
    "filtered_threads",
    con,
    db_path,
    num_partitions=120,
    max_workers=90,
)
get_subreddit_distribution("threads", con)
get_subreddit_distribution("filtered_threads", con)
get_author_distribution_parallel(
    "threads", con, db_path, num_partitions=120, max_workers=90
)
get_author_distribution_parallel(
    "filtered_threads",
    con,
    db_path,
    num_partitions=120,
    max_workers=90,
)

with open("../data/saved_stats.json", "r") as f:
    existing_data = json.load(f)
distribution = existing_data["subreddit_distribution_threads"]
subreddits = [
    key
    for key, value in sorted(distribution.items(), key=lambda x: x[1], reverse=True)[:5]
]
for subreddit in subreddits:
    create_subreddit_tables(con, subreddit)
    table_stats_parallel(f"{subreddit}_lookup", con, num_workers=90, chunk_size=10000)
    calculate_weighted_average(f"thread_lengths_{subreddit}_lookup")
    calculate_weighted_average(f"thread_widths_{subreddit}_lookup")
    calculate_weighted_average(f"all_widths_{subreddit}_lookup")
    get_depth_distribution(f"{subreddit}_threads", con)
    get_depth_distribution(f"filtered_{subreddit}_threads", con)
    get_thread_lengths(f"{subreddit}_threads", con)
    get_thread_lengths(f"filtered_{subreddit}_threads", con)
    get_number_of_threads(f"{subreddit}_threads", con)
    get_number_of_threads(f"filtered_{subreddit}_threads", con)
    get_thread_score_distribution_parallel(
        f"{subreddit}_threads", con, db_path, num_partitions=120, max_workers=90
    )
    get_thread_score_distribution_parallel(
        f"filtered_{subreddit}_threads",
        con,
        db_path,
        num_partitions=120,
        max_workers=90,
    )
    get_author_distribution_parallel(
        f"{subreddit}_threads", con, db_path, num_partitions=120, max_workers=90
    )
    get_author_distribution_parallel(
        f"filtered_{subreddit}_threads",
        con,
        db_path,
        num_partitions=120,
        max_workers=90,
    )


monitoring_active = False
monitor_thread.join()  # optional, if you want to ensure it has stopped before exiting
log_with_resources("Script finished")
con.commit()
con.close()
