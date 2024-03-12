import logging
from psycopg2 import DatabaseError
from connect import create_connect

sql = """
drop table if exists tasks;
drop table if exists users;
drop table if exists status;

create table users (
	id SERIAL primary key,
	fullname VARCHAR(100),
	email VARCHAR(100) unique 
);

create table status (
	id SERIAL primary key,
	name VARCHAR(50) unique
);
insert into status (name)
values ('new'), ('in progress'), ('completed');

create table tasks (
	id SERIAL primary key,
	title VARCHAR(100),
	description text,
	status_id INTEGER,
	user_id INTEGER,
	foreign key (status_id) references status (id),
	foreign key (user_id) references users (id)
		on delete cascade
);
"""


def create_table(conn, sql):
    c = conn.cursor()
    try:
        c.execute(sql)
        conn.commit()
    except DatabaseError as er:
        logging.error(f"Database error: {er}")
        print(er)
        conn.rollback()
    finally:
        c.close()


if __name__ == "__main__":
    try:
        with create_connect() as conn:
            create_table(conn, sql)
    except RuntimeError as er:
        logging.error(f"Runtime error: {er}")
    except DatabaseError as er:
        logging.error(f"Database error: {er}")
