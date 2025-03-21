import mysql.connector
from mysql.connector import Error
import json
import logging
from typing import Optional, Dict, Any
from dataclasses import dataclass
from enum import Enum


class TaskStatus(Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    FAILED = "failed"
    COMPLETED = "completed"


@dataclass
class TaskQueueConfig:
    db_name: str
    db_user: str
    db_password: str
    db_host: str
    db_table: str = "tasks"
    max_attempts: int = 5
    delete_completed_after_days: int = 7
    delete_failed_after_days: int = 30


class TaskQueue:
    def __init__(self, config: TaskQueueConfig):
        """
        Initializes TaskQueue.

        :param config: TaskQueue configuration.
        """
        self.config = config
        self.conn = None
        self._connect()
        self._ensure_table_exists()

    def _connect(self):
        """Establishes a connection to the database."""
        try:
            self.conn = mysql.connector.connect(
                user=self.config.db_user,
                password=self.config.db_password,
                host=self.config.db_host,
                database=self.config.db_name
            )
            self.conn.autocommit = True  # Disable autocommit for transaction control
            logging.info("Successfully connected to the database.")
        except Error as e:
            logging.error(f"Error connecting to the database: {e}")
            raise

    def _ensure_table_exists(self):
        """
        Checks if the table exists and creates it if necessary.
        """
        cursor = self.conn.cursor()
        try:
            # Динамически получаем значения статусов из Enum
            status_values = [status.value for status in TaskStatus]
            status_enum_str = ", ".join(f"'{status}'" for status in status_values)

            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.config.db_table} (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    task_name VARCHAR(255) NOT NULL,
                    args JSON NOT NULL,
                    status ENUM({status_enum_str}) NOT NULL DEFAULT %s,
                    count_attempts INT NOT NULL DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX status_index (status)  -- Добавляем индекс на столбец status
                )
            """, (TaskStatus.PENDING.value,))
            logging.info(f"Table {self.config.db_table} successfully created or already exists.")
        except Error as e:
            logging.error(f"Error creating table: {e}")
            raise
        finally:
            cursor.close()

    def add_task(self, task_name: str, args: Dict[str, Any]):
        """
        Adds a new task to the queue.

        :param task_name: Task name.
        :param args: Task arguments (must be JSON-serializable).
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"""
                INSERT INTO {self.config.db_table} (task_name, args)
                VALUES (%s, %s)
            """, (task_name, json.dumps(args)))
            logging.info(f"Task '{task_name}' added.")
        except Error as e:
            logging.error(f"Error adding task: {e}")
            raise
        finally:
            cursor.close()

    def fetch_task(self) -> Optional[Dict[str, Any]]:
        """
        Fetches a task for execution.

        :return: A dictionary with task data or None if no tasks are available.
        """
        cursor = self.conn.cursor(dictionary=True)
        try:
            # Start a transaction to select the task
            self.conn.start_transaction()

            # Select the task
            cursor.execute(f"""
                SELECT * FROM {self.config.db_table}
                WHERE (status = %s OR (status = %s AND count_attempts < %s))
                ORDER BY id
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            """, (TaskStatus.PENDING.value, TaskStatus.FAILED.value, self.config.max_attempts))
            task = cursor.fetchone()

            if task:
                # Update the task status
                self.update_task_status(task['id'], TaskStatus.PROCESSING)
                logging.debug(f"Task {task['id']} taken for execution.")

            # Commit the transaction
            self.conn.commit()

            return task

        except Error as e:
            # Rollback the transaction in case of an error
            self.conn.rollback()
            logging.error(f"Error fetching task: {e}")
            raise
        finally:
            # Close the cursor
            cursor.close()

    def update_task_status(self, task_id: int, status: TaskStatus):
        """
        Updates the status of a task.

        :param task_id: Task ID.
        :param status: New task status.
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"""
                UPDATE {self.config.db_table}
                SET status = %s,
                    count_attempts = count_attempts + 1,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (status.value, task_id))
            logging.debug(f"Task {task_id} status updated to {status.value}.")
        except Error as e:
            logging.error(f"Error updating task status: {e}")
            raise
        finally:
            cursor.close()

    def cleanup_tasks(self):
        """
        Cleans up old tasks:
        - Tasks with status 'completed' older than delete_completed_after_days days.
        - Tasks with status 'failed' and count_attempts >= max_attempts older than delete_failed_after_days days.
        """
        cursor = self.conn.cursor()
        try:
            # Delete completed tasks
            cursor.execute(f"""
                DELETE LOW_PRIORITY FROM {self.config.db_table}
                WHERE status = %s AND created_at < DATE_ADD(NOW(), INTERVAL -%s DAY)
            """, (TaskStatus.COMPLETED.value, self.config.delete_completed_after_days))
            completed_count = cursor.rowcount

            # Delete failed tasks
            cursor.execute(f"""
                DELETE LOW_PRIORITY FROM {self.config.db_table}
                WHERE status = %s AND count_attempts >= %s AND created_at < DATE_ADD(NOW(), INTERVAL -%s DAY)
            """, (TaskStatus.FAILED.value, self.config.max_attempts, self.config.delete_failed_after_days))
            failed_count = cursor.rowcount

            logging.info(f"Deleted {completed_count} completed and {failed_count} failed tasks.")
        except Error as e:
            logging.error(f"Error deleting tasks: {e}")
            raise
        finally:
            cursor.close()

    def __del__(self):
        """Closes the database connection when the object is deleted."""
        if self.conn:
            self.conn.close()
            logging.info("Database connection closed.")