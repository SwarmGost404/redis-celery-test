import time
import logging
from task_queue import TaskQueue, TaskQueueConfig, TaskStatus

# Настройка логирования
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")


def worker(config: TaskQueueConfig):
    """
    Воркер для выполнения задач.

    :param config: Конфигурация TaskQueue.
    """
    task_queue = TaskQueue(config)
    while True:
        task = task_queue.fetch_task()
        if task:
            try:
                # Выполняем задачу (заглушка)
                logging.info(f"Выполняется задача {task['id']}: {task['task_name']}")
                # Обновляем статус задачи на 'completed'
                task_queue.update_task_status(task['id'], TaskStatus.COMPLETED)
            except Exception as e:
                logging.error(f"Ошибка при выполнении задачи {task['id']}: {e}")
                # Обновляем статус задачи на 'failed'
                task_queue.update_task_status(task['id'], TaskStatus.FAILED)
        else:
            # Если задач нет, ждем 1 секунду
            time.sleep(1)
            

if __name__ == "__main__":
    config = TaskQueueConfig(
        db_name="mydatabase",
        db_user="rq",
        db_password="Server",
        db_host="localhost"
    )
    worker(config)