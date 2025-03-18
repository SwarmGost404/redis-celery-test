import mysql.connector
import json
import time

def execute_task(task_name, args):
    """
    Выполняет задачу.
    """
    if task_name == 'add':
        return sum(args)
    elif task_name == 'multiply':
        result = 1
        for num in args:
            result *= num
        return result
    else:
        raise ValueError(f"Unknown task: {task_name}")

def fetch_task():
    """
    Берет задачу из очереди и блокирует её для других workers.
    """
    conn = mysql.connector.connect(
        user='rq',
        password='Server',
        host='localhost',
        database='mydatabase'
    )
    cursor = conn.cursor(dictionary=True)

    # Берем задачу со статусом 'pending' и блокируем её
    cursor.execute("""
        SELECT * FROM tasks 
        WHERE status = 'pending' 
        ORDER BY created_at 
        LIMIT 1 
        FOR UPDATE SKIP LOCKED
    """)
    task = cursor.fetchone()

    if task:
        # Обновляем статус задачи на 'processing'
        cursor.execute("UPDATE tasks SET status = 'processing' WHERE id = %s", (task['id'],))
        conn.commit()

    # Закрытие соединения
    cursor.close()
    conn.close()

    return task

def update_task_status(task_id, status):
    """
    Обновляет статус задачи.
    """
    conn = mysql.connector.connect(
        user='rq',
        password='Server',
        host='localhost',
        database='mydatabase'
    )
    cursor = conn.cursor()

    if status == 'completed':
        # Удаляем задачу, если она выполнена
        cursor.execute("DELETE FROM tasks WHERE id = %s", (task_id,))
    else:
        # Обновляем статус задачи
        cursor.execute("UPDATE tasks SET status = %s WHERE id = %s", (status, task_id))

    conn.commit()

    # Закрытие соединения
    cursor.close()
    conn.close()

def worker():
    """
    Worker, который берет задачи из очереди и выполняет их.
    """
    while True:
        task = fetch_task()

        if task:
            try:
                # Выполняем задачу
                result = execute_task(task['task_name'], json.loads(task['args']))
                print(f"Task {task['id']} completed with result: {result}")

                # Обновляем статус задачи на 'completed' и удаляем её
                update_task_status(task['id'], 'completed')
            except Exception as e:
                print(f"Task {task['id']} failed with error: {e}")

                # Обновляем статус задачи на 'failed'
                update_task_status(task['id'], 'failed')
        else:
            # Если задач нет, ждем 1 секунду
            time.sleep(1)

# Запуск worker
if __name__ == "__main__":
    worker()