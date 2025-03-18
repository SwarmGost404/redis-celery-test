from celery import Celery

# Инициализация Celery
app = Celery('tasks', 
             broker='redis://localhost:6379/0',
             
             )
app.conf.result_backend = 'redis://localhost:6379/0'

# Простая задача
@app.task
def add(x, y):
    return x + y