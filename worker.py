import time
import logging
from task_queue import TaskQueue, TaskQueueConfig, TaskStatus

# logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")


def worker(config: TaskQueueConfig):
    """
    Worker for executing tasks.
    :param config: TaskQueue configuration.
    """
    task_queue = TaskQueue(config)
    while True:
        task = task_queue.fetch_task()
        if task:
            try:
                logging.info(f"Выполняется задача {task['id']}: {task['task_name']}")
                # status 'completed'
                task_queue.update_task_status(task['id'], TaskStatus.COMPLETED)
            except Exception as e:
                logging.error(f"Ошибка при выполнении задачи {task['id']}: {e}")
                # status 'failed'
                task_queue.update_task_status(task['id'], TaskStatus.FAILED)
        else:
            # if not exists task
            time.sleep(1)
            

if __name__ == "__main__":
    config = TaskQueueConfig(
        db_name="mydatabase",
        db_user="rq",
        db_password="Server",
        db_host="localhost"
    )
    worker(config)