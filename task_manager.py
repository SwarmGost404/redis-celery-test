import mysql.connector
import json

def add_task(task_name, args):
    """
    Добавляет задачу в очередь.
    """
    # Подключение к MariaDB
    conn = mysql.connector.connect(
        user='rq',
        password='Server',
        host='localhost',
        database='mydatabase'
    )
    cursor = conn.cursor()

    # Добавление задачи
    query = "INSERT INTO tasks (task_name, args, status) VALUES (%s, %s, 'pending')"
    cursor.execute(query, (task_name, json.dumps(args)))
    conn.commit()

    # Закрытие соединения
    cursor.close()
    conn.close()
