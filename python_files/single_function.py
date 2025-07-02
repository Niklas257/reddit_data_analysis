import duckdb
import json
import threading
import time
from create_database import (
    create_subreddit_tables,
)
from stats import (
    get_depth_distribution,
    get_number_of_threads,
    get_thread_score_distribution,
    table_stats,
    calculate_weighted_average,
    get_thread_lengths,
    get_author_distribution,
    log_with_resources,
    calculate_variance,
)
from filter_database import filter_threads, create_testing_threads
from get_samples import generate_jsonl_from_threads

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
con.execute("SET threads TO 20;")
con.execute("PRAGMA verify_parallelism;")
con.execute("PRAGMA memory_limit='30GB';")
log_with_resources("threads set to 20")

filter_threads(
    con,
    "threads",
    "training_threads",
    num_authors=2,
    min_authors=True,
    check_english=True,
)
create_testing_threads(
    con, "training_threads", "testing_threads", num_threads_per_category=20
)
generate_jsonl_from_threads(con, "training_threads", "../data/training_threads.jsonl")
generate_jsonl_from_threads(
    con, "testing_threads", "../data/testing_threads.jsonl", testing=True
)

monitoring_active = False
monitor_thread.join()  # optional, if you want to ensure it has stopped before exiting
log_with_resources("Script finished")
con.commit()
con.close()
