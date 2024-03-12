import logging
import os
import psycopg2
from psycopg2 import DatabaseError
from contextlib import contextmanager
from dotenv import load_dotenv
from pathlib import Path
from connect import create_connect


load_dotenv(Path(__file__).parent.parent / ".env")


def get_data(conn, sql, params=None):
    data = None
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        data = cur.fetchall()
    except DatabaseError as er:
        logging.error(f"Database error: {er}")
    finally:
        cur.close()
    return data


def create_data(conn, sql, params=None):
    id = None
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        conn.commit()
        id = cur.fetchone()[0]
    except DatabaseError as er:
        logging.error(f"Database error: {er}")
        conn.rollback()
    finally:
        cur.close()

    return id


def change_data(conn, sql, params=None):
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        conn.commit()
    except DatabaseError as er:
        logging.error(f"Database error: {er}")
        conn.rollback()
    finally:
        cur.close()


def delete_data(conn, sql, params=None):
    is_deleted = False
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        is_deleted = cur.rowcount > 0
        conn.commit()
    except DatabaseError as er:
        logging.error(f"Database error: {er}")
        conn.rollback()
    finally:
        cur.close()

    return is_deleted


def get_tasks_by_user_id(conn, user_id):
    sql = """
    SELECT * FROM tasks WHERE user_id = %s
    """
    return get_data(conn, sql, (user_id,))


def get_task_by_id(conn, task_id):
    sql = """
    select * from tasks
    where id = %s;
    """
    return get_data(conn, sql, (task_id,))


def get_tasks_by_status(conn, status):
    sql = """
    select * from tasks where status_id in 
    (select id from status where name = %s);
    """
    return get_data(conn, sql, (status,))


def change_task_status(conn, task_id, new_status_id):
    sql = """
    UPDATE tasks
    SET status_id = %s
    WHERE id = %s;
    """
    change_data(conn, sql, (new_status_id, task_id))
    return get_task_by_id(conn, task_id)


def get_users_without_tasks(conn):
    sql = """
    select * from users
    where id not in (select user_id from tasks where user_id = users.id); 
    """
    return get_data(conn, sql)


def create_task(conn, title, description, status_id, user_id):
    sql = """
    INSERT INTO tasks (title, description, 
    status_id, user_id) VALUES (%s, %s, %s, %s)
    RETURNING id;
    """
    id = create_data(conn, sql, (title, description, status_id, user_id))
    return get_task_by_id(conn, id)


def get_not_completed_tasks(conn):
    sql = """
    select * from tasks
    where not status_id = 3;
    """

    return get_data(conn, sql)


def delete_task_by_id(conn, task_id):
    sql = """
    delete from tasks where id = %s;
    """

    is_deleted = delete_data(conn, sql, (task_id,))

    if is_deleted > 0:
        return f"Task with {task_id} id deleted"
    else:
        return f"No task found with {task_id} id"


def get_users_by_email(conn, email):
    sql = """
    select * from users where email like %s;
    """

    return get_data(conn, sql, (f"%{email}%",))


def change_user_name(conn, user_id, new_user_name):
    sql = """
    update users
    set fullname = %s
    where id = %s;
    """

    change_data(conn, sql, (new_user_name, user_id))
    return get_user_by_id(conn, user_id)


def get_user_by_id(conn, user_id):
    sql = """
    select * from users
    where id = %s;
    """

    return get_data(conn, sql, (user_id,))


def get_count_tasks_by_status(conn):
    sql = """
    select s.id, s.name, count(*) as task_count from tasks t
    left join status s on t.status_id = s.id
    group by s.id
    order by s.id;
    """

    return get_data(conn, sql)


def get_tasks_by_user_email_domain(conn, domain):
    sql = """
    select t.*, u.fullname as user_fullname, u.email as user_email
    from tasks t
    inner join users u on t.user_id = u.id
    where u.email like %s;
    """

    return get_data(conn, sql, (f"%{domain}",))


def get_tasks_without_description(conn):
    sql = """
    select * from tasks
    where description is null;
    """

    return get_data(conn, sql)


def get_users_and_tasks_by_status(conn, status):
    sql = """
    select u.*, t.id as task_id, t.title, t.description, t.status_id from users u
    inner join tasks t on t.user_id = u.id and t.status_id in (
	    select id from status
	    where name = %s
    )
    """

    return get_data(conn, sql, (status,))


def get_count_tasks_by_users(conn):
    sql = """
    select u.*, coalesce(count(t.user_id), 0) as task_count from users u
    left join tasks t on t.user_id = u.id
    group by u.id;
    """

    return get_data(conn, sql)


if __name__ == "__main__":
    try:
        with create_connect() as conn:

            print(get_tasks_by_user_id(conn, 1))
            print(get_tasks_by_status(conn, "new"))
            print(change_task_status(conn, 3, 2))
            print(get_users_without_tasks(conn))
            print(create_task(conn, "new task", "description", 1, 2))
            print(get_not_completed_tasks(conn))
            print(delete_task_by_id(conn, 2))
            print(get_users_by_email(conn, ".com"))
            print(change_user_name(conn, 1, "Hans Peter"))
            print(get_count_tasks_by_status(conn))
            print(get_tasks_by_user_email_domain(conn, "@example.net"))
            print(get_tasks_without_description(conn))
            print(get_users_and_tasks_by_status(conn, "new"))
            print(get_count_tasks_by_users(conn))

    except RuntimeError as er:
        logging.error(f"Runtime error: {er}")
    except DatabaseError as er:
        logging.error(f"Database error: {er}")
