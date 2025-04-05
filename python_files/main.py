import duckdb
import json
from create_database import (
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
)

from filter_database import make_threads_unique, filter_threads


con = duckdb.connect("../data/database.db")

create_row_counts_table(con)

create_lookup_table(con)

table_stats("lookup_table", con)
calculate_weighted_average("thread_lengths_lookup_table")
calculate_weighted_average("thread_widths_lookup_table")
calculate_weighted_average("all_widths_lookup_table")

create_threads_table(con, "all_threads")
make_threads_unique(con, "threads")
filter_threads(con, "threads")

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

con.commit()
con.close()
