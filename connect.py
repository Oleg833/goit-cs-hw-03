import os
import logging
import psycopg2
from psycopg2 import DatabaseError
from contextlib import contextmanager
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(__file__).parent / ".env")


@contextmanager
def create_connect():
    conn_params = {
        "host": os.getenv("POSTGRES_HOST"),
        "database": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
    }
    conn = None
    try:
        conn = psycopg2.connect(**conn_params)
        print("Connection successful")
        try:
            yield conn
        except:
            conn.close()

    except psycopg2.OperationalError as er:
        print(f"Connection failed, {er}")
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    load_dotenv(Path(__file__).parent / ".env")
    try:
        with create_connect() as conn:
            pass
    except RuntimeError as er:
        logging.error(f"Runtime error: {er}")
    except DatabaseError as er:
        logging.error(f"Database error: {er}")
