import random
import logging
import os
import psycopg2
from psycopg2 import DatabaseError
from faker import Faker
from contextlib import contextmanager
from dotenv import load_dotenv
from pathlib import Path
from connect import create_connect

load_dotenv(Path(__file__).parent.parent / ".env")

# Ініціалізація Faker
fake = Faker()


if __name__ == "__main__":
    try:
        with create_connect() as conn:

            cur = conn.cursor()

            for _ in range(10):
                fullname = fake.name()
                email = fake.email()
                cur.execute(
                    "INSERT INTO users (fullname, email) VALUES (%s, %s)",
                    (fullname, email),
                )

            for _ in range(30):
                title = fake.sentence(nb_words=5)
                description = fake.text()
                status_id = random.randint(1, 3)
                user_id = random.randint(1, 10)
                cur.execute(
                    "INSERT INTO tasks (title, description, status_id, user_id) VALUES (%s, %s, %s, %s)",
                    (title, description, status_id, user_id),
                )
            conn.commit()
            cur.close()
            conn.close()

    except RuntimeError as er:
        logging.error(f"Runtime error: {er}")
    except DatabaseError as er:
        logging.error(f"Database error: {er}")
