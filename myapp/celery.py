from celery import Celery

# Создаем экземпляр Celery
app = Celery(
    'myapp',  # Имя приложения
    broker='db+sqlite:///celery.db',  # URL брокера (SQLite)
    include=['myapp.tasks']  # Указываем модуль с задачами
)

# Настройки Celery (опционально)
app.conf.update(
    result_backend='db+sqlite:///celery.db',  # Бэкенд для хранения результатов
    task_serializer='json',  # Сериализация задач в JSON
    accept_content=['json'],  # Принимать только JSON
    result_serializer='json',  # Сериализация результатов в JSON
    timezone='Europe/Moscow',  # Часовой пояс
    enable_utc=True,  # Использовать UTC
)