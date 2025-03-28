import duckdb
from create_database import (
    add_initial_tables,
    add_comments_to_comments_tables,
    create_lookup_table,
    create_subreddit_tables,
    create_threads_table,
)
from stats import (
    create_row_counts_table,
    create_filtered_row_counts,
    analyze_thread_score_distribution,
    get_subreddit_distribution,
    table_stats,
    calculate_weighted_average,
    get_thread_lengths,
)
from filter_database import make_threads_unique, filter_threads

con = duckdb.connect("database_sample.db")
add_initial_tables(con, "posts.csv", "comments.csv")
add_comments_to_comments_tables(con, "comments.csv")

create_row_counts_table(con)

create_lookup_table(con)

table_stats("lookup_table", con)
calculate_weighted_average("thread_lengths_lookup_table")
calculate_weighted_average("thread_widths_lookup_table")
calculate_weighted_average("all_widths_lookup_table")

create_threads_table(con)
make_threads_unique(con)
filter_threads(con)

create_filtered_row_counts("filtered_lookup", con)
create_filtered_row_counts("filtered_threads", con)
get_thread_lengths("filtered_threads", con)

# Todo: File to save the outputs
analyze_thread_score_distribution("threads", con)
analyze_thread_score_distribution("filtered_threads", con)
get_subreddit_distribution("threads", con)
get_subreddit_distribution("filtered_threads", con)

subreddits = ["AskReddit"]
for subreddit in subreddits:
    create_subreddit_tables(con, subreddit)
    table_stats(f"{subreddit}_lookup", con)
    calculate_weighted_average(f"thread_lengths_{subreddit}_lookup")
    calculate_weighted_average(f"thread_widths_{subreddit}_lookup")
    calculate_weighted_average(f"all_widths_{subreddit}_lookup")
    table_stats(f"{subreddit}_threads", con)
    table_stats(f"filtered_{subreddit}_threads", con)

con.commit()
con.close()
