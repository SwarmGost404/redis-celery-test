import mysql.connector
import json
import time
from mysql.connector import Error

class TaskQueue:
    def __init__(self, user, password, host, database, table_name='tasks'):
        """
        Initializes the TaskQueue with database connection details and table name.

        :param user: Database username.
        :param password: Database password.
        :param host: Database host.
        :param database: Database name.
        :param table_name: Name of the table to store tasks (default: 'tasks').
        """
        self.user = user
        self.password = password
        self.host = host
        self.database = database
        self.table_name = table_name
        self.conn = None
        self._ensure_table_exists()

    def _connect(self):
        """Establishes a connection to the database."""
        try:
            self.conn = mysql.connector.connect(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database
            )
        except Error as e:
            print(f"Database connection error: {e}")
            raise

    def _ensure_table_exists(self):
        """
        Ensures that the tasks table exists in the database.
        If it doesn't, creates the table.
        """
        self._connect()
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    task_name VARCHAR(255) NOT NULL,
                    args JSON NOT NULL,
                    status VARCHAR(50) NOT NULL DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            self.conn.commit()
        except Error as e:
            print(f"Error creating table: {e}")
            raise
        finally:
            cursor.close()
            self.conn.close()

    def add_task(self, task_name, args):
        """
        Adds a new task to the queue.

        :param task_name: Name of the task (e.g., 'add', 'multiply').
        :param args: Arguments for the task (must be JSON-serializable).
        """
        self._connect()
        cursor = self.conn.cursor()
        try:
            query = f"""
                INSERT INTO {self.table_name} (task_name, args, status)
                VALUES (%s, %s, 'pending')
            """
            cursor.execute(query, (task_name, json.dumps(args)))
            self.conn.commit()
        except Error as e:
            print(f"Error adding task: {e}")
            raise
        finally:
            cursor.close()
            self.conn.close()

    def fetch_task(self):
        """
        Fetches a task from the queue and locks it for processing.

        :return: A dictionary representing the task, or None if no tasks are available.
        """
        self._connect()
        cursor = self.conn.cursor(dictionary=True)
        try:
            # Start a transaction
            self.conn.start_transaction()

            # Fetch and lock a task
            cursor.execute(f"""
                SELECT * FROM {self.table_name}
                WHERE status = 'pending'
                ORDER BY created_at
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            """)
            task = cursor.fetchone()

            if task:
                # Update the task status to 'processing'
                cursor.execute(f"""
                    UPDATE {self.table_name}
                    SET status = 'processing'
                    WHERE id = %s
                """, (task['id'],))
                self.conn.commit()

            return task
        except Error as e:
            print(f"Error fetching task: {e}")
            self.conn.rollback()
            raise
        finally:
            cursor.close()
            self.conn.close()

    def update_task_status(self, task_id, status):
        """
        Updates the status of a task.

        :param task_id: ID of the task to update.
        :param status: New status of the task (e.g., 'completed', 'failed').
        """
        self._connect()
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"""
                UPDATE {self.table_name}
                SET status = %s
                WHERE id = %s
            """, (status, task_id))
            self.conn.commit()
        except Error as e:
            print(f"Error updating task status: {e}")
            self.conn.rollback()
            raise
        finally:
            cursor.close()
            self.conn.close()


def execute_task(task_name, args):
    """
    Executes a task based on its name and arguments.

    :param task_name: Name of the task (e.g., 'add', 'multiply').
    :param args: Arguments for the task.
    :return: Result of the task execution.
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


def worker(task_queue):
    """
    Worker that fetches tasks from the queue and executes them.

    :param task_queue: An instance of TaskQueue.
    """
    while True:
        task = task_queue.fetch_task()
        if task:
            try:
                # Execute the task
                result = execute_task(task['task_name'], json.loads(task['args']))
                print(f"Task {task['id']} completed with result: {result}")

                # Update the task status to 'completed'
                task_queue.update_task_status(task['id'], 'completed')
            except Exception as e:
                print(f"Task {task['id']} failed with error: {e}")

                # Update the task status to 'failed'
                task_queue.update_task_status(task['id'], 'failed')
        else:
            # Wait if no tasks are available
            time.sleep(1)


if __name__ == "__main__":
    # Initialize TaskQueue with database connection details
    task_queue = TaskQueue(user='rq', password='Server', host='localhost', database='mydatabase')

    # Start the worker
    worker(task_queue)