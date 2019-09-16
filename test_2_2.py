'''
2. На любом языке программирования реализовать сервис по сбору агрегатов из таблицы БД с банковскими транзакциями.
    Агрегаты должны быть собраны из реконсилированных данных по каждому клиенту в разрезах дней, месяцев и общий итог.

'''


#Импортируем необходимые пакеты
import psycopg2
import test_2_1_map_reduce #импортируем и выполняем реконсиляцию

#Подключаемся к БД
conn = psycopg2.connect(dbname='postgres', user='root', host='localhost')
cursor = conn.cursor()

#Получаем агрегированные данные, сгруппированные  по пользователю, году и месяцу: сумму транзакций, минимальное, максимальное и среднее значение транзакции
cursor.execute('SELECT user_guid, EXTRACT("year" FROM transaction_date), EXTRACT("month" FROM transaction_date), SUM(transaction_amount), MIN(transaction_amount), MAX(transaction_amount), AVG(transaction_amount) '
               'FROM test_2_1.transactions '
               'GROUP BY user_guid, EXTRACT("year" FROM transaction_date), EXTRACT("month" FROM transaction_date) ORDER BY user_guid, EXTRACT("year" FROM transaction_date), EXTRACT("month" FROM transaction_date);')
records = cursor.fetchall()
for record in records:
    print(record)
