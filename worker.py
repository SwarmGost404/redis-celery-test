import mysql.connector
import json
import time
from mysql.connector import Error

class TaskQueue:
    def __init__(self, user, password, host, database):
        self.user = user
        self.password = password
        self.host = host
        self.database = database
        self.conn = None
        self._ensure_table_exists()

    def _connect(self):
        """Устанавливает соединение с базой данных."""
        try:
            self.conn = mysql.connector.connect(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database
            )
        except Error as e:
            print(f"Ошибка подключения к базе данных: {e}")
            raise

    def _ensure_table_exists(self):
        """Проверяет, существует ли таблица tasks, и создаёт её, если нет."""
        self._connect()
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tasks (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    task_name VARCHAR(255) NOT NULL,
                    args JSON NOT NULL,
                    status VARCHAR(50) NOT NULL DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            self.conn.commit()
        except Error as e:
            print(f"Ошибка при создании таблицы: {e}")
            raise
        finally:
            cursor.close()
            self.conn.close()

    def fetch_task(self):
        """
        Атомарно получает задачу из очереди и блокирует её для других workers.
        """
        self._connect()
        cursor = self.conn.cursor(dictionary=True)

        try:
            # Начинаем транзакцию
            self.conn.start_transaction()

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
                self.conn.commit()

            return task
        except Error as e:
            print(f"Ошибка при получении задачи: {e}")
            self.conn.rollback()
            raise
        finally:
            cursor.close()
            self.conn.close()

    def update_task_status(self, task_id, status):
        """
        Обновляет статус задачи.
        """
        self._connect()
        cursor = self.conn.cursor()

        try:
            if status == 'completed':
                # Удаляем задачу, если она выполнена
                cursor.execute("DELETE FROM tasks WHERE id = %s", (task_id,))
            else:
                # Обновляем статус задачи
                cursor.execute("UPDATE tasks SET status = %s WHERE id = %s", (status, task_id))

            self.conn.commit()
        except Error as e:
            print(f"Ошибка при обновлении статуса задачи: {e}")
            self.conn.rollback()
            raise
        finally:
            cursor.close()
            self.conn.close()

    def execute_task(self, task_name, args):
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
            raise ValueError(f"Неизвестная задача: {task_name}")

    def worker(self):
        """
        Worker, который берет задачи из очереди и выполняет их.
        """
        while True:
            task = self.fetch_task()

            if task:
                try:
                    # Выполняем задачу
                    result = self.execute_task(task['task_name'], json.loads(task['args']))
                    print(f"Задача {task['id']} выполнена с результатом: {result}")

                    # Обновляем статус задачи на 'completed' и удаляем её
                    self.update_task_status(task['id'], 'completed')
                except Exception as e:
                    print(f"Задача {task['id']} завершилась с ошибкой: {e}")

                    # Обновляем статус задачи на 'failed'
                    self.update_task_status(task['id'], 'failed')
            else:
                # Если задач нет, ждем 1 секунду
                time.sleep(1)


# Пример использования
if __name__ == "__main__":
    # Инициализация TaskQueue с данными для подключения к MySQL
    task_queue = TaskQueue(user='rq', password='Server', host='localhost', database='mydatabase')

    # Запуск worker
    task_queue.worker()