import mysql.connector
from mysql.connector import Error
import json
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from config import TaskQueueConfig, TaskStatus


class TaskQueue:
    def __init__(self, config: TaskQueueConfig):
        """
        Инициализация TaskQueue.

        :param config: Конфигурация TaskQueue.
        """
        self.config = config
        self.conn = None
        self._connect()
        self._ensure_table_exists()

    def _connect(self):
        """Устанавливает соединение с базой данных."""
        try:
            self.conn = mysql.connector.connect(
                user=self.config.db_user,
                password=self.config.db_password,
                host=self.config.db_host,
                database=self.config.db_name
            )
            logging.info("Успешное подключение к базе данных.")
        except Error as e:
            logging.error(f"Ошибка подключения к базе данных: {e}")
            raise

    def _ensure_table_exists(self):
        """
        Проверяет, существует ли таблица, и создает её, если необходимо.
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.config.db_table} (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    task_name VARCHAR(255) NOT NULL,
                    args JSON NOT NULL,
                    status ENUM('pending', 'processing', 'failed', 'completed') NOT NULL DEFAULT 'pending',
                    count_attempts INT NOT NULL DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                )
            """)
            self.conn.commit()
            logging.info(f"Таблица {self.config.db_table} успешно создана или уже существует.")
        except Error as e:
            logging.error(f"Ошибка при создании таблицы: {e}")
            self.conn.rollback()
            raise
        finally:
            cursor.close()

    def add_task(self, task_name: str, args: Dict[str, Any]):
        """
        Добавляет новую задачу в очередь.

        :param task_name: Имя задачи.
        :param args: Аргументы задачи (должны быть JSON-сериализуемы).
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"""
                INSERT INTO {self.config.db_table} (task_name, args)
                VALUES (%s, %s)
            """, (task_name, json.dumps(args)))
            self.conn.commit()
            logging.info(f"Задача '{task_name}' добавлена.")
        except Error as e:
            logging.error(f"Ошибка при добавлении задачи: {e}")
            self.conn.rollback()
            raise
        finally:
            cursor.close()

    def fetch_task(self) -> Optional[Dict[str, Any]]:
        """
        Получает задачу для выполнения.

        :return: Словарь с данными задачи или None, если задач нет.
        """
        cursor = self.conn.cursor(dictionary=True)
        try:
            # Начало транзакции для выбора задачи
            self.conn.start_transaction()

            # Выбор задачи
            cursor.execute(f"""
                SELECT * FROM {self.config.db_table}
                WHERE (status = %s OR (status = %s AND count_attempts < %s))
                ORDER BY id
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            """, (TaskStatus.PENDING.value, TaskStatus.FAILED.value, self.config.max_attempts))
            task = cursor.fetchone()

            # Фиксация транзакции (освобождаем блокировку)
            self.conn.commit()

            if task:
                # Обновление статуса задачи в отдельной транзакции
                self.update_task_status(task['id'], TaskStatus.PROCESSING)
                logging.debug(f"Задача {task['id']} взята на выполнение.")

            return task

        except Error as e:
            # Откат транзакции в случае ошибки
            self.conn.rollback()
            logging.error(f"Ошибка при получении задачи: {e}")
            raise
        finally:
            # Закрытие курсора
            cursor.close()

    def update_task_status(self, task_id: int, status: TaskStatus):
        """
        Обновляет статус задачи.

        :param task_id: ID задачи.
        :param status: Новый статус задачи.
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"""
                UPDATE {self.config.db_table}
                SET status = %s,
                    count_attempts = count_attempts + 1,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (status.value, task_id))  # Используем status.value
            self.conn.commit()
            logging.debug(f"Статус задачи {task_id} изменен на {status.value}.")
        except Error as e:
            logging.error(f"Ошибка при обновлении статуса задачи: {e}")
            self.conn.rollback()
            raise
        finally:
            cursor.close()
            
    def cleanup_tasks(self):
        """
        Удаляет старые задачи:
        - Задачи со статусом 'completed' старше delete_completed_after_days дней.
        - Задачи со статусом 'failed' и count_attempts >= max_attempts старше delete_failed_after_days дней.
        """
        cursor = self.conn.cursor()
        try:
            # Удаляем завершенные задачи
            cursor.execute(f"""
                DELETE FROM {self.config.db_table}
                WHERE status = %s AND created_at < %s
            """, (TaskStatus.COMPLETED.name, datetime.now() - timedelta(days=self.config.delete_completed_after_days)))
            completed_count = cursor.rowcount

            # Удаляем проваленные задачи
            cursor.execute(f"""
                DELETE FROM {self.config.db_table}
                WHERE status = %s AND count_attempts >= %s AND created_at < %s
            """, (TaskStatus.FAILED.name, self.config.max_attempts, datetime.now() - timedelta(days=self.config.delete_failed_after_days)))
            failed_count = cursor.rowcount

            self.conn.commit()
            logging.info(f"Удалено {completed_count} завершенных и {failed_count} проваленных задач.")
        except Error as e:
            logging.error(f"Ошибка при удалении задач: {e}")
            self.conn.rollback()
            raise
        finally:
            cursor.close()

    def __del__(self):
        """Закрывает соединение с базой данных при удалении объекта."""
        if self.conn:
            self.conn.close()
            logging.info("Соединение с базой данных закрыто.")