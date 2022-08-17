import psycopg2

# Подключаемся к базе данных
con = psycopg2.connect(database='muzika_db', user='postgres', password='')
cur = con.cursor()
print("database opened successfully")
cur.execute("""
DROP TABLE phones;
DROP TABLE client;
""")


def create_db():
    '''Функция, создающая структуру БД (таблицы)'''
    # Создаем курсор для выполнения операций с базой данных
    cur = con.cursor()
    dates_great_client = """ CREATE TABLE IF NOT EXISTS client
    (id SERIAL PRIMARY KEY,
    name VARCHAR(25) NOT NULL,
    last_name VARCHAR(30) NOT NULL,
    email VARCHAR(30) NOT NULL); """
    cur.execute(dates_great_client)

    dates_great_phones = '''CREATE TABLE IF NOT EXISTS phones
    (id SERIAL PRIMARY KEY,
    client_id INTEGER NOT NULL REFERENCES client(id),
    phone VARCHAR(15));'''
    cur.execute(dates_great_phones)
    con.commit()
    print("Таблицы  созданы")


create_db()


def add_client(dates):
    '''Функция, позволяющая добавить нового клиента'''
    cur = con.cursor()
    new_client = """ INSERT INTO client(name, last_name, email) VALUES (%s, %s, %s) """
    cur.executemany(new_client, dates)
    con.commit()
    print(cur.rowcount, "Записи о клиентах вставлены в таблицу client")


dates_to_insert = [('Александр', 'Поляков', 'alexpol@mail.ru'), ('Ольга', 'Румянцева', 'olerum82@mail.ru'),
                   ('Матвей', 'Захаров', 'matzah65@mail.ru')]
add_client(dates_to_insert)


def add_phone(dates_phones):
    '''Функция, позволяющая добавить телефон для существующего клиента'''
    cur = con.cursor()
    phone_for_client = """ INSERT INTO phones(client_id, phone) VALUES (%s, %s) """
    cur.executemany(phone_for_client, dates_phones)
    con.commit()
    print(cur.rowcount, "Записи о телефонах клиентов вставлены в таблицу phones")


dates_phones_insert = [(1, "89853255577"), (2, "89853258888"), (3, " "), (2, "89151472536"), (2, "89159999999")]
add_phone(dates_phones_insert)


def change_client(id, email):
    '''Функция, позволяющая изменить данные о клиенте'''
    cur = con.cursor()
    print("Таблица до изменения данных о клиенте")
    sql_table = """select * from client where id = %s"""
    cur.execute(sql_table, (id,))
    res = cur.fetchone()
    print(res)
    '''Обновим данные по e-mail'''
    set_update = """UPDATE client set email = %s where id = %s"""
    cur.execute(set_update, (email, id))
    con.commit()
    print(cur.rowcount, "Запись в таблице обновлена")
    print("Таблица после изменения данных о клиенте")
    sql_table = """select * from client where id = %s"""
    cur.execute(sql_table, (id,))
    res = cur.fetchone()
    print(res)


change_client(1, "bifone78@mail.ru")


def delete_phone(id):
    '''Функция, позволяющая удалить телефон для существующего клиента'''
    cur = con.cursor()
    print("Таблица до изменения данных о клиенте")
    sql_table_p = """select * from phones where id = %s"""
    cur.execute(sql_table_p, (id,))
    res = cur.fetchone()
    print(res)
    sql_delete = """ DELETE from phones where id = %s"""
    cur.execute(sql_delete, (id,))
    con.commit()
    print(cur.rowcount, "Телефон удален")


delete_phone(2)


# def delete_client(id):
#     '''Функция, позволяющая удалить существующего клиента'''
#     cur = con.cursor()
#     print("Таблица до изменения данных о клиенте")
#     sql_table_c = """select * from client where id = %s"""
#     cur.execute(sql_table_c, (id,))
#     res = cur.fetchone()
#     print(res)
#     sql_delete_client = """ DELETE from client c WHERE NOT EXISTS (SELECT id=%s from phones p where c.id = p.client_id)"""
#     # sql_delete_client = """DELETE FROM client WHERE id IN (SELECT id FROM phones WHERE client_id = %s)"""
#     cur.execute(sql_delete_client, (id,))
#     con.commit()
#     print(cur.rowcount, "Запись(и) о клиенте удалена(ы)")
#     print("Таблица после изменения данных о клиенте")
#     sql_table_c = """select * from client where id = %s"""
#     cur.execute(sql_table_c, (id,))
#     res = cur.fetchone()
#     print(res)
#
#
# delete_client(2)


def delete_client(name, last_name):
    '''Функция, позволяющая удалить существующего клиента'''
    cur = con.cursor()
    cur.execute('''
    SELECT id FROM client WHERE name=%s AND last_name=%s;
    ''', (name, last_name))
    res = cur.fetchone()[0]
    print(res)
    cur.execute('''
    DELETE FROM phones WHERE client_id=%s;
    ''', (res,))
    cur.execute('''
    DELETE FROM client WHERE name=%s AND last_name=%s
    ''', (name, last_name))


delete_client('Ольга', 'Румянцева')


def find_client(name=None, last_name=None, email=None, phone=None, **kwargs):
    """Функция, позволяющая найти клиента по его данным (имени, фамилии, email-у или телефону)"""
    cur = con.cursor()
    sql_find = """SELECT name, last_name, email, p.phone from client as c left join phones as p 
    on c.id=p.client_id WHERE name=%s or last_name=%s or email=%s or p.phone=%s"""
    cur.execute(sql_find, (name, last_name, email, phone))
    res = cur.fetchall()
    print(res)


find_client(name="Матвей")
# # with psycopg2.connect(database="clients_db", user="postgres", password="postgres") as con:
# #     pass  # вызывайте функции здесь
# #
con.close()