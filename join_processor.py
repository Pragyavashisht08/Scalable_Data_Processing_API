import sqlite3
import pandas as pd
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

DB_PATH = os.path.join("output", "join_database.db")
USERS_CSV = "users.csv"
TRANSACTIONS_CSV = "transactions.csv"
OUTPUT_CSV = os.path.join("output", "result.csv")


def load_csv_to_sqlite(csv_path, table_name, conn, chunksize=100_000):
    """
    Loads a large CSV file into SQLite in chunks.
    This avoids loading the entire file into memory.
    """
    logging.info(f"Loading {csv_path} into table {table_name}")

    first_chunk = True

    for chunk in pd.read_csv(csv_path, chunksize=chunksize):
        chunk.to_sql(
            table_name,
            conn,
            if_exists="replace" if first_chunk else "append",
            index=False
        )

        first_chunk = False
        logging.info(f"Inserted chunk into {table_name}")

    logging.info(f"Finished loading {table_name}")


def perform_join():
    """
    Performs INNER JOIN between users and transactions using SQLite.
    Writes the result to output/result.csv in chunks.
    """


    logging.info("Join job started")

    # Ensure output directory exists
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    conn = sqlite3.connect(DB_PATH)

    try:
        load_csv_to_sqlite(USERS_CSV, "users", conn)
        load_csv_to_sqlite(TRANSACTIONS_CSV, "transactions", conn)

        logging.info("Creating index on users.user_id")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_users_user_id ON users(user_id)")

        logging.info("Creating index on transactions.user_id")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_transactions_user_id ON transactions(user_id)")

        conn.commit()

        query = """
        SELECT 
            t.transaction_id,
            t.user_id,
            u.name,
            u.signup_date,
            t.amount
        FROM transactions t
        INNER JOIN users u
        ON t.user_id = u.user_id
        """

        logging.info("Executing join query")

        first_chunk = True

        for chunk in pd.read_sql_query(query, conn, chunksize=100_000):
            chunk.to_csv(
                OUTPUT_CSV,
                mode="w" if first_chunk else "a",
                index=False,
                header=first_chunk
            )

            first_chunk = False
            logging.info(f"Written one joined chunk to {OUTPUT_CSV}")

        logging.info(f"Join job completed successfully. {OUTPUT_CSV} created.")

    finally:
        conn.close()

if __name__ == "__main__":
    perform_join()
