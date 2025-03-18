from tasks import add

# Запуск нескольких задач
results = [add.delay(i, i) for i in range(10)]

# Получение результатов
for result in results:
    print(result.get())