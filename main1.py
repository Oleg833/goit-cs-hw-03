import logging
from psycopg2 import DatabaseError
from connect import create_connect
from pprint import pprint
from classes import User, Task


def format_data_into_dict(columns, data):
    result = []

    for row in data:
        record = {}
        for idx, column in enumerate(columns):
            record[column] = row[idx]
        result.append(record)

    return result


def get_data(conn, sql, params=None):
    data = None

    c = conn.cursor()
    try:
        c.execute(sql, params)
        data = c.fetchall()
    except DatabaseError as er:
        logging.error(f"Database error: {er}")
    finally:
        c.close()

    columns = [description[0] for description in c.description]

    if data:
        return format_data_into_dict(columns, data)
    else:
        return None


def create_data(conn, sql, params=None):
    id = None
    c = conn.cursor()
    try:
        c.execute(sql, params)
        conn.commit()
        id = c.fetchone()[0]
    except DatabaseError as er:
        logging.error(f"Database error: {er}")
        conn.rollback()
    finally:
        c.close()

    return id


def change_data(conn, sql, params=None):
    c = conn.cursor()
    try:
        c.execute(sql, params)
        conn.commit()
    except DatabaseError as er:
        logging.error(f"Database error: {er}")
        conn.rollback()
    finally:
        c.close()


def delete_data(conn, sql, params=None):
    is_deleted = False
    c = conn.cursor()
    try:
        c.execute(sql, params)
        is_deleted = c.rowcount > 0
        conn.commit()
    except DatabaseError as er:
        logging.error(f"Database error: {er}")
        conn.rollback()
    finally:
        c.close()

    return is_deleted


def get_task_by_id(conn, task_id):
    sql = """
    select * from tasks
    where id = %s;
    """

    return get_data(conn, sql, (task_id,))


def get_tasks_by_user_id(conn, user_id):
    sql = """
    select * from tasks
    where user_id = %s; 
    """

    return get_data(conn, sql, (user_id,))


def get_tasks_by_status(conn, status):
    sql = """
    select * from tasks
    where status_id in (select id from status where name = %s);
    """

    return get_data(conn, sql, (status,))


def change_task_status(conn, task_id, new_status_id):
    sql = """
    update tasks
    set status_id = %s
    where id = %s;
    """

    change_data(conn, sql, (new_status_id, task_id))
    return get_task_by_id(conn, task_id)


def get_users_without_tasks(conn):
    sql = """
    select * from users
    where id not in (select user_id from tasks where user_id = users.id); 
    """

    return get_data(conn, sql)


def create_task(conn, new_task: Task):
    sql = """
    insert into tasks (title, description, status_id, user_id)
    values (%s, %s, %s, %s)
    RETURNING id;
    """

    id = create_data(
        conn,
        sql,
        (new_task.title, new_task.description, new_task.status_id, new_task.user_id),
    )
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


def get_user_by_id(conn, user_id):
    sql = """
    select * from users
    where id = %s;
    """

    return get_data(conn, sql, (user_id,))


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
            """
            Отримати всі завдання певного користувача. Використайте SELECT для отримання завдань конкретного користувача за його user_id.
            """
            # pprint(get_tasks_by_user_id(conn, 1))

            """Вибрати завдання за певним статусом. Використайте підзапит для вибору завдань з конкретним статусом, наприклад, 'new'."""
            # pprint(get_tasks_by_status(conn, "new"))

            """
            Оновити статус конкретного завдання. Змініть статус конкретного завдання на 'in progress' або інший статус.
            """
            # pprint(change_task_status(conn, 1, 2))

            """
            Отримати список користувачів, які не мають жодного завдання. Використайте комбінацію SELECT, WHERE NOT IN і підзапит.
            """
            # pprint(get_users_without_tasks(conn))

            """
            Додати нове завдання для конкретного користувача. Використайте INSERT для додавання нового завдання.
            """
            # pprint(create_task(conn, Task("new task", "complete a new task", 1)))

            """
            Отримати всі завдання, які ще не завершено. Виберіть завдання, чий статус не є 'завершено'.
            """
            # pprint(get_not_completed_tasks(conn))

            """
            Видалити конкретне завдання. Використайте DELETE для видалення завдання за його id.
            """
            # print(delete_task_by_id(conn, 105))

            """
            Знайти користувачів з певною електронною поштою. Використайте SELECT із умовою LIKE для фільтрації за електронною поштою.
            """
            # pprint(get_users_by_email(conn, ".com"))

            """
            Оновити ім'я користувача. Змініть ім'я користувача за допомогою UPDATE.
            """
            # pprint(change_user_name(conn, 1, "Mango Mangovich"))

            """
            Отримати кількість завдань для кожного статусу. Використайте SELECT, COUNT, GROUP BY для групування завдань за статусами.

            """
            pprint(get_count_tasks_by_status(conn))

            """
            Отримати завдання, які призначені користувачам з певною доменною частиною електронної пошти. Використайте SELECT з умовою LIKE в поєднанні з JOIN, щоб вибрати завдання, призначені користувачам, чия електронна пошта містить певний домен (наприклад, '%@example.com').
            """
            # pprint(get_tasks_by_user_email_domain(conn, "@example.com"))

            """
            Отримати список завдань, що не мають опису. Виберіть завдання, у яких відсутній опис.
            """
            # pprint(get_tasks_without_description(conn))

            """
            Вибрати користувачів та їхні завдання, які є у статусі 'in progress'. Використайте INNER JOIN для отримання списку користувачів та їхніх завдань із певним статусом.
            """
            # pprint(get_users_and_tasks_by_status(conn, "new"))

            """
            Отримати користувачів та кількість їхніх завдань. Використайте LEFT JOIN та GROUP BY для вибору користувачів та підрахунку їхніх завдань.
            """
            # pprint(get_count_tasks_by_users(conn))

    except RuntimeError as er:
        logging.error(f"Runtime error: {er}")
    except DatabaseError as er:
        logging.error(f"Database error: {er}")