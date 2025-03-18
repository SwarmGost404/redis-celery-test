from tasks import add, multiply


# Вызов задачи
result = add.delay(4, 6)
print(result.get())  # Ожидаемый результат: 10

result = multiply.delay(4, 6)
print(result.get())  # Ожидаемый результат: 24