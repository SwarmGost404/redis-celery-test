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