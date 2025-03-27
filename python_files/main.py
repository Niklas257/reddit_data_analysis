import duckdb
from create_database import add_initial_tables, add_comments_to_comments_tables, create_lookup_table, create_subreddit_tables
from stats import create_row_counts_table

con = duckdb.connect("database.db")
add_initial_tables(con, "posts.csv", "comments.csv")
add_comments_to_comments_tables(con, "comments.csv")

create_row_counts_table(con)

create_lookup_table(con)

subreddits = ["AskReddit"]

for subreddit in subreddits:
    create_subreddit_tables(con, subreddit)

con.commit()
con.close()
