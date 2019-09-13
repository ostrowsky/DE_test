'''
Задание 2:
    1. На любом языке программирования реализовать реконсиляцию транзакций клиентов банка из двух источников (как минимум один - таблица БД, второй на выбор).
	В данных должен присутствовать уникальный ключ - uid/id на выбор;
	Реконсиляция должна быть масштабируема и применима к Big Data

	Дополнительно сделать реконсиляцию:
	- для разных типов данных (дата, текст, числа)
	- для числовых данных должна быть возможность сконфигурировать толеранс (допустимую погрешность в %)
'''


#Импортируем необходимые пакеты
import json
import psycopg2
from datetime import datetime
from functools import reduce


#Определеяем функцию INSERT
def insert(transaction):
    cursor.execute(
        'INSERT INTO test_2_1.transactions (transaction_id, index, user_guid, transaction_amount, transaction_date) VALUES( %s, %s, %s,%s, %s);',
        (transaction['transaction_id'], transaction['index'], transaction['user_guid'],
         transaction['transaction_amount'], transaction['transaction_date']))

#Определяем функцию DELETE
def delete(index):
    cursor.execute('DELETE  FROM test_2_1.transactions WHERE index = %(index)s;', {'index': index})

#Определяем функцию UPDATE
def update(**kwargs):
    sql_template = "UPDATE test_2_1.transactions SET ({}) = %s WHERE index = {}"
    sql = sql_template.format(', '.join(kwargs.keys()), kwargs['index'])
    params = (tuple(kwargs.values()),)
    cursor.execute(sql, params)


#Отдельные проверки реализовал в виде отдельных функций для удобства последующих доработок и сопровождения
#Определяем проверку наличия в БД транзакции из источника
def transaction_check(db_data, transaction):
    if len(db_data) == 0:
        error = 'transaction {} not found'.format(transaction['transaction_id'])

        #Вставляем недостающую транзакцию
        insert(transaction)
        return error

#Определяем проверку соответствия ID пользователя в БД  источнику
def user_guid_check(json_data, db_data):
    if json_data['user_guid']!= db_data:
        error = 'User_guid {} not valid'.format(db_data)

        #Исправляем несоответствие ID пользователя в БД
        index_upd = json_data['index']
        correct_guid = json_data['user_guid']
        update(index=index_upd, user_guid=correct_guid)
        return error

#Определяем проверку соответствия суммы транзакции в БД  источнику
def amount_check(json_data, db_data, tolerance):
    if abs(json_data['transaction_amount'] - db_data)/json_data['transaction_amount'] * 100 > tolerance:
        error = '{:.2%}'.format(abs(json_data['transaction_amount']- db_data)/json_data['transaction_amount'] ) #Определяем процент расхождения между суммами в источнике и в БД

        # Исправляем несоответствие суммы в БД
        index_upd = json_data['index']
        correct_amount = json_data['transaction_amount']
        update(index=index_upd, transaction_amount=correct_amount)
        return error

#Определяем проверку соответствия даты транзакции в БД  источнику
def date_check(json_data, db_data):
    if datetime.strptime(json_data['transaction_date'], '%Y-%m-%d %H:%M:%S') != db_data:
        error = {'Date {}'.format(db_data): 'Not valid'}

        # Исправляем несоответствие даты в БД
        index_upd = json_data['index']
        correct_date = json_data['transaction_date']
        update(index=index_upd, transaction_date=correct_date)
        return error

def reconciliation(json_data, tolerance=1):
    #Создаем список для хранения выявленных ошибок
    errors = []
    counter = 0


    #Выполняем сверки для каждой транзакции из JSON
    for transaction in json_data:
        id = transaction['transaction_id']
        cursor.execute('SELECT * FROM test_2_1.transactions WHERE transaction_id = %(id)s;', {'id': id})
        record = cursor.fetchall()

        # Вызываем проверку наличия в БД транзакции из источника
        transaction_error = transaction_check(record, transaction)
        if not transaction_error:
            # Если транзакция есть, вызываем проверку соответствия ID пользователя в БД  источнику
            db_guid = record[0][2]
            user_guid_error = user_guid_check(transaction,  db_guid)

            # Если транзакция есть, вызываем проверку соответствия суммы транзакции в БД  источнику
            db_amount = record[0][3]
            amount_error = amount_check(transaction, db_amount, tolerance)

            # Если транзакция есть, вызываем проверку соответствия датыы транзакции в БД  источнику
            db_date = record[0][4]
            date_error = date_check(transaction, db_date)

        #Если выявлена какая-либо ошибка, помещаем запись о ней в список
        if (transaction_error is not None) or (user_guid_error is not None) or (amount_error is not None) or (date_error is not None):
            errors.append({})
            errors[counter]['transaction_id'] = id

            #Записываем ошибку отсутствия транзакции
            if transaction_error is not None:
                errors[counter]['Transaction_error'] = transaction_error
                transaction_error = None

            # Записываем ошибку несоответствия ID пользователя
            if user_guid_error is not None:
                errors[counter]['User_guid_error'] = user_guid_error
                user_guid_error = None

            # Записываем ошибку несоответствия суммы транзакции
            if amount_error is not None:
                errors[counter]['Amount error'] = amount_error
                amount_error = None

        # Записываем ошибку несоответствия даты транзакции
            if date_error is not None:
                errors[counter]['Date error'] = date_error
                date_error = None

            counter += 1

    #Возвращаем протокол ошибок
    return errors

    # Определяем функцию разбиения источника данных на отдельные массивы для параллельной реконсилляции

#Определяем функцию разбиения данных источника на отдельные массивы
def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]

#Определяем функцию сборки результатов отдельных экземпляров реконсиляции в общий протокол ошибок
def reducer(*args):
    total_errors = []
    for arg in args:
        total_errors.extend(arg)
    return total_errors
    

#Открываем JSON с источником данных: ID транзакции, порядковый индекс транзакции, ID прользователя, сумма транзакции, дата транзакции
with open('test_2_1.json') as f:
    json_data = json.load(f)

#Подключаемся к БД
conn = psycopg2.connect(dbname='postgres', user='root', host='localhost')
conn.autocommit = True
cursor = conn.cursor()

#Удаляем и создаем таблицу
cursor.execute('DROP TABLE IF EXISTS test_2_1.transactions')
cursor.execute('CREATE TABLE  test_2_1.transactions (transaction_id varchar, index bigint, user_guid uuid, transaction_amount float4, transaction_date timestamp);')

#Заполняем таблицу из JSON
for transaction in json_data:
    insert(transaction)

#Вносим искусственное расхождение в ID пользователя
index_upd = 0
wrong_guid = '60e6f6a8-e889-400c-a39c-bad5405517b2'
update(index=index_upd, user_guid=wrong_guid, )

#Вносим искусственное расхождение - удаляем транзакцию
index_del = 1
delete(index=index_del)

#Вносим искусственное расхождение в сумму транзакции
index_upd = 301
wrong_amount = 2000
update(index=index_upd, transaction_amount=wrong_amount)

#Вносим искусственное расхождение в дату транзакции
index_upd = 601
wrong_date = '2013-06-15 07:20:57'
update(index=index_upd, transaction_date=wrong_date)

#Вносим несколько расхождений в транзакцию
index_upd = 901
wrong_guid = '60e6f6a8-e889-400c-a39c-bad5405517b2'
wrong_amount = 2000
wrong_date = '2013-06-15 07:20:57'
update(index=index_upd, user_guid=wrong_guid, transaction_amount=wrong_amount, transaction_date=wrong_date)

#Задаем допустимую погрешность в %
tolerance = 1

#Запускаем последовательно отдельные экземпляры реконсиляции для отдельных массивов данных источника (разбиваем исходный JSON на массивы по 300 записей). Это обеспечивает масштабируемость реконсиляции  и ее применимость к Big Data, т.к. при больших объемах данных,
#данные от источника могут быть разделены на отдельные массивы и поданы на вход экземпляров реконсиляции, запущенных на отдельных вычислительных узлах, после чего результаты экземпляров рекосниляции собираетются в общий протокола ошибок
mapped = map(reconciliation, chunks(json_data, 300))

#Собираем протоколы ошибок из отдельных экземпляров реконсиляции в общий протокол
reduced = reduce(reducer, mapped)
print(reduced)

#Сохраняем общий протокол ошибок в файл JSON
with open('errors.json', 'w') as f:
    json.dump(reduced, f)

