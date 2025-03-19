import mysql.connector
import json

def add_task(task_name, args):
    """
    Adds a task to the queue.
    """
    # Connect to MariaDB
    conn = mysql.connector.connect(
        user='rq',
        password='Server',
        host='localhost',
        database='mydatabase'
    )
    cursor = conn.cursor()

    # Add the task
    query = "INSERT INTO tasks (task_name, args, status) VALUES (%s, %s, 'pending')"
    cursor.execute(query, (task_name, json.dumps(args)))
    conn.commit()

    # Close the connection
    cursor.close()
    conn.close()