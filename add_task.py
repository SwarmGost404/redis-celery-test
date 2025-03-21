import logging
from task_queue import TaskQueue, TaskQueueConfig
from typing import Dict, Any

# logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def add_task(config: TaskQueueConfig, task_name: str, args: Dict[str, Any]):
    """
        Adds a task to the queue.
        :param config: TaskQueue configuration.
        :param task_name: Task name.
        :param args: Task arguments.
    """
    task_queue = TaskQueue(config)
    task_queue.add_task(task_name, args)


if __name__ == "__main__":
    config = TaskQueueConfig(
        db_name="mydatabase",
        db_user="rq",
        db_password="Server",
        db_host="localhost"
    )
    add_task(config, "example_task", {"param1": "value1", "param2": 42})