from create_database import filter_valid_tables


# Create row_counts table in stats.db
def create_row_counts_table(con):
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS row_counts (
            table_name TEXT,
            row_count BIGINT
        )
    """
    )

    tables = con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
    ).fetchall()

    tables = filter_valid_tables(tables)

    for (table_name,) in tables:
        # Get count from main database
        row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        # Insert into stats database
        con.execute(
            f"INSERT INTO row_counts VALUES ('{table_name}', {row_count})"
        )
