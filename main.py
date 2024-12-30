#!/usr/bin/env python3

import psycopg2
import pandas as pd
from psycopg2.extras import execute_values
from datetime import datetime
import os


# Создание подключения к PostgreSQL
conn = psycopg2.connect(database = 'db', user = 'hseguest', password = 'hsepassword', host = 'rc1b-o3ezvcgz5072sgar.mdb.yandexcloud.net', port = '6432')

try:
    # Создание курсора
    with conn.cursor() as cursor:
        # Сброс состояния транзакции на случай ошибки
        conn.rollback()

        # Чтение данных из таблицы
        cursor.execute("SELECT * FROM info.clients")
        records_cl = cursor.fetchall()
        names = [x[0] for x in cursor.description]

    # Преобразование в DataFrame
    clients = pd.DataFrame(records_cl, columns=names)
    values = list(clients.itertuples(index=False, name=None))
    columns = ', '.join(clients.columns)

    with conn.cursor() as c:
        c.execute("TRUNCATE TABLE public.aiai_stg_clients CASCADE;")
        conn.commit()
        execute_values(
            cur=c,
            sql="""
                INSERT INTO public.aiai_stg_clients
                (client_id, last_name, first_name,	patronymic,	date_of_birth,	passport_num,	passport_valid_to,	phone,	create_dt,	update_dt)
                VALUES %s
                ON CONFLICT DO NOTHING;
                """,
            argslist=values
        )
    conn.commit()
    
except Exception as e:
    conn.rollback() 
    print(f"Ошибка: {e}")

#Clients
try:
    # Создание курсора
    with conn.cursor() as c:

        # Сброс состояния транзакции на случай ошибки
        conn.rollback()
        c.execute("TRUNCATE TABLE public.aiai_dwh_dim_clients_hist CASCADE;")
        conn.commit()

        # Чтение данных из таблицы
        c.execute("SELECT client_id, last_name, first_name, patronymic, date_of_birth,	passport_num, passport_valid_to, phone,	create_dt as effective_from, cast('2999-12-31'as date) as effective_to, CASE WHEN update_dt IS NOT NULL THEN 'True' ELSE 'False' END as deleted_flg FROM public.aiai_stg_clients")
        records = c.fetchall()
        names = [x[0] for x in c.description]
    
    dwh_clients = pd.DataFrame(records, columns=names)
    val_clients = list(dwh_clients.itertuples(index=False, name=None))
    # columns = ', '.join(dwh_clients.columns)

    with conn.cursor() as c:
        execute_values(
            cur=c,
            sql="""
                INSERT INTO public.aiai_dwh_dim_clients_hist
                (client_id,last_name,first_name,patronymic,date_of_birth,passport_num,passport_valid_to,phone,effective_from,effective_to,deleted_flg)
                VALUES %s
                ON CONFLICT DO NOTHING;
                """,
            argslist=val_clients
        )
    conn.commit()

except Exception as e:
    conn.rollback() 
    print(f"Ошибка: {e}")


try:
    # Создание курсора
    with conn.cursor() as cursor:
        # Сброс состояния транзакции на случай ошибки
        conn.rollback()

        # Чтение данных из таблицы
        cursor.execute("SELECT * FROM info.accounts")
        records_acc = cursor.fetchall()
        names = [x[0] for x in cursor.description]

    # Преобразование в DataFrame
    accounts = pd.DataFrame(records_acc, columns=names)
    values = list(accounts.itertuples(index=False, name=None))
    columns = ', '.join(accounts.columns)

    with conn.cursor() as c:
        c.execute("TRUNCATE TABLE public.aiai_stg_accounts CASCADE;")
        conn.commit()
        execute_values(
            cur=c,
            sql="""
                INSERT INTO public.aiai_stg_accounts
                (account,	valid_to,	client,	create_dt,	update_dt)
                VALUES %s
                ON CONFLICT DO NOTHING;
                """,
            argslist=values
        )
    conn.commit()
    
except Exception as e:
    conn.rollback() 
    print(f"Ошибка: {e}")

#Accounts
conn = psycopg2.connect(database = 'db', user = 'hseguest', password = 'hsepassword', host = 'rc1b-o3ezvcgz5072sgar.mdb.yandexcloud.net', port = '6432')
try:
    # Создание курсора
    with conn.cursor() as c:
        # Сброс состояния транзакции на случай ошибки
        conn.rollback()
        c.execute("TRUNCATE TABLE public.aiai_dwh_dim_accounts_hist CASCADE;")
        conn.commit()


        # Чтение данных из таблицы
        c.execute("SELECT account, valid_to, client, create_dt, update_dt FROM public.aiai_stg_accounts")
        records = c.fetchall()
        names = [x[0] for x in c.description]
    
    dwh_accounts = pd.DataFrame(records, columns=names)
    val_accounts = list(dwh_accounts.itertuples(index=False, name=None))
    # columns = ', '.join(dwh_accounts.columns)

    with conn.cursor() as c:
        execute_values(
            cur=c,
            sql="""
                INSERT INTO public.aiai_dwh_dim_accounts_hist
                (account,valid_to,client,create_dt,update_dt)
                VALUES %s
                ON CONFLICT DO NOTHING;
                """,
            argslist=val_accounts
        )
    conn.commit()

except Exception as e:
    conn.rollback() 
    print(f"Ошибка: {e}")

try:
    # Создание курсора
    with conn.cursor() as cursor:
        # Сброс состояния транзакции на случай ошибки
        conn.rollback()

        # Чтение данных из таблицы
        cursor.execute("SELECT * FROM info.cards")
        records_card = cursor.fetchall()
        names = [x[0] for x in cursor.description]

    # Преобразование в DataFrame
    cards = pd.DataFrame(records_card, columns=names)
    values = list(cards.itertuples(index=False, name=None))
    columns = ', '.join(cards.columns)

    with conn.cursor() as c:
        c.execute("TRUNCATE TABLE public.aiai_stg_cards CASCADE;")
        conn.commit()
        execute_values(
            cur=c,
            sql="""
                INSERT INTO public.aiai_stg_cards
                (card, account, create_dt, update_dt)
                VALUES %s
                ON CONFLICT DO NOTHING;
                """,
            argslist=values
        )
    conn.commit()
    
except Exception as e:
    conn.rollback() 
    print(f"Ошибка: {e}")

#Cards
conn = psycopg2.connect(database = 'db', user = 'hseguest', password = 'hsepassword', host = 'rc1b-o3ezvcgz5072sgar.mdb.yandexcloud.net', port = '6432')
try:
    # Создание курсора
    with conn.cursor() as c:
        # Сброс состояния транзакции на случай ошибки
        conn.rollback()
        c.execute("TRUNCATE TABLE public.aiai_dwh_dim_cards_hist CASCADE;")
        conn.commit()

        # Чтение данных из таблицы
        c.execute("SELECT card, account, create_dt, update_dt FROM public.aiai_stg_cards")
        records = c.fetchall()
        names = [x[0] for x in c.description]
    
    dwh_cards = pd.DataFrame(records, columns=names)
    dwh_cards['card'] = dwh_cards['card'].str.strip()
    val_cards = list(dwh_cards.itertuples(index=False, name=None))
    columns = ', '.join(dwh_cards.columns)

    with conn.cursor() as c:
        execute_values(
            cur=c,
            sql="""
                INSERT INTO public.aiai_dwh_dim_cards_hist
                (card, account, create_dt, update_dt)
                VALUES %s
                ON CONFLICT DO NOTHING;
                """,
            argslist=val_cards
        )
    conn.commit()

except Exception as e:
    conn.rollback()

def convert_to_datetime(value):
    return pd.to_datetime(value, format='%Y-%m-%d %H:%M:%S', errors='coerce') 
    

# Путь к папке с файлами
folder_path = r"/"

# Список для хранения дат
dates = []

# Проходим по файлам в папке
for file_name in os.listdir(folder_path):
    # Проверяем, что файл начинается с "terminals_" 
    if file_name.startswith("terminals_"): 
        # Извлекаем дату
        date_str = file_name.replace("terminals_", "").replace(".xlsx", "") 
        # Добавляем дату в массив
        dates.append(date_str)

# dates = ['01032021', '02032021', '03032021']
terminals = {}
transactions = {}
blacklist = {}
dwh_terminals = pd.DataFrame(columns=['terminal_id','terminal_type','terminal_city','terminal_address', 'effective_from', 'effective_to', 'deleted_flg'])
conn = psycopg2.connect(database = 'db', user = 'hseguest', password = 'hsepassword', host = 'rc1b-o3ezvcgz5072sgar.mdb.yandexcloud.net', port = '6432')
with conn.cursor() as c:
    c.execute("""
        TRUNCATE TABLE public.aiai_dwh_fact_passport_blacklist CASCADE;
        TRUNCATE TABLE public.aiai_dwh_fact_transactions CASCADE;
        TRUNCATE TABLE public.aiai_dwh_dim_terminals_hist CASCADE;
    """)  
    conn.commit() 


for dt in dates:
    key = f'{dt}'
    terminals[key] = pd.read_excel(fr'/terminals_{dt}.xlsx')
    transactions[key] = pd.read_csv(fr'/transactions_{dt}.txt', sep = ';', converters={
        'amount': lambda x: float(x.replace(',', '.')),
        'transaction_date': convert_to_datetime
        })
    blacklist[key] = pd.read_excel(fr'/passport_blacklist_{dt}.xlsx')
    
    # Запись terminals  в БД
    try:
        values = list(terminals[key].itertuples(index=False, name=None))
        columns = ', '.join(terminals[key].columns)
        with conn.cursor() as c:
            #Очистка таблицы STG_terminals
            c.execute("TRUNCATE TABLE public.aiai_stg_terminals CASCADE;")
            conn.commit()

            execute_values(
                cur=c,
                sql="""
                    INSERT INTO public.aiai_stg_terminals
                    (terminal_id,terminal_type,terminal_city,terminal_address)
                    VALUES %s
                    ON CONFLICT DO NOTHING;
                    """,
                argslist=values
            )

            # Чтение данных из таблицы aiai_stg_terminals
            c.execute("SELECT terminal_id,terminal_type,terminal_city,terminal_address from public.aiai_stg_terminals")
            conn.commit()
            records = c.fetchall()
            names = [x[0] for x in c.description]
            #Запись данных из STG в DF
            dwh_terminals_n = pd.DataFrame(records, columns=names)
            dwh_terminals_n['effective_from'] = datetime.strptime(key, "%d%m%Y")
            dwh_terminals_n['effective_to'] = datetime.strptime("31122999 00:00:00", "%d%m%Y %H:%M:%S")
            dwh_terminals_n['deleted_flg'] = 'False'
            dwh_terminals = pd.concat([dwh_terminals, dwh_terminals_n], ignore_index=True)
                        

    except Exception as e:
        conn.rollback() 
        print(f"Ошибка: {e}")
   

    # Запись blacklist  в БД
    try:
        values = list(blacklist[key].itertuples(index=False, name=None))
        columns = ', '.join(blacklist[key].columns)
        with conn.cursor() as c:

            #Очистка таблицы STG
            c.execute("TRUNCATE TABLE public.aiai_stg_passport_blacklist CASCADE;")
            conn.commit()

            execute_values(
                cur=c,
                sql="""
                    INSERT INTO public.aiai_stg_passport_blacklist
                    (entry_dt, passport_num)
                    VALUES %s
                    """,
                argslist=values
            )

            # Чтение данных из таблицы aiai_stg_black_list
            c.execute("SELECT entry_dt, passport_num from public.aiai_stg_passport_blacklist")
            conn.commit()
            records = c.fetchall()
            names = [x[0] for x in c.description]
            #Запись данных из STG в DF
            dwh_bl = pd.DataFrame(records, columns=names)
        
    
    except Exception as e:
        conn.rollback() 
        print(f"Ошибка: {e}")
    
    


    # Запись transactions
    try:
        values = list(transactions[key].itertuples(index=False, name=None))
        columns = ', '.join(transactions[key].columns)
        with conn.cursor() as c:

            #Очистка таблицы STG
            c.execute("TRUNCATE TABLE public.aiai_stg_transactions CASCADE;")
            conn.commit()

            #Запись в STG
            execute_values(
                cur=c,
                sql="""
                    INSERT INTO public.aiai_stg_transactions
                    (trans_id,trans_date,amt, card, oper_type, oper_result, terminal)
                    VALUES %s
                    ON CONFLICT DO NOTHING;
                    """,
                argslist=values
            )
        
            # Чтение данных из таблицы aiai_stg_transactions
            c.execute("SELECT trans_id,trans_date,amt, card, oper_type, oper_result, terminal from public.aiai_stg_transactions")
            conn.commit()
            records = c.fetchall()
            names = [x[0] for x in c.description]
            #Запись данных из STG в DF
            dwh_transactions = pd.DataFrame(records, columns=names)
            values = list(dwh_transactions.itertuples(index=False, name=None))
            columns = ', '.join(dwh_transactions.columns)

            #Запись данных из DF в DWH
            execute_values(
                cur=c,
                sql="""
                    INSERT INTO public.aiai_dwh_fact_transactions
                    (trans_id,trans_date,amt, card, oper_type, oper_result, terminal)
                    VALUES %s
                    ON CONFLICT DO NOTHING;
                    """,
                argslist=values
            )
        conn.commit()
    
    except Exception as e:
        conn.rollback() 
        print(f"Ошибка: {e}")

val_terminals = list(dwh_terminals.itertuples(index=False, name=None))

# #Запись данных из DF terminals в DWH terminals
with conn.cursor() as c:
    execute_values(
        cur=c,
        sql="""
            INSERT INTO public.aiai_dwh_dim_terminals_hist
            (terminal_id,terminal_type,terminal_city,terminal_address, effective_from, effective_to, deleted_flg)
            VALUES %s;
            """,
        argslist=val_terminals
    )
conn.commit()

val_bl = list(dwh_bl.itertuples(index=False, name=None))
# columns = ', '.join(dwh_bl.columns)

#Запись данных из DF в DWH
with conn.cursor() as c:
    execute_values(
        cur=c,
        sql="""
            INSERT INTO public.aiai_dwh_fact_passport_blacklist
            (entry_dt, passport_num)
            VALUES %s
            ON CONFLICT DO NOTHING;
            """,
        argslist=val_bl
    )

conn.commit()
c.close()

with conn.cursor() as c:
    c.execute("TRUNCATE TABLE public.aiai_rep_fraud")
    conn.commit()

    c.execute("""
                with t4 as (
                with t3 as (
                with t2 as (
                with t1 as (
                select tr.trans_id, tr.card, tr.trans_date, tr.oper_type, tr.amt, tr.oper_result, tr.terminal, crd.account, crd.create_dt, crd.update_dt from aiai_dwh_fact_transactions tr
                left join aiai_dwh_dim_cards_hist crd  on tr.card = crd.card)
                select trans_id, card, trans_date, oper_type, amt, oper_result, terminal, t1.account, t1.create_dt as crd_create_dt, acc.client, acc.valid_to as acc_valid_to, acc.create_dt as acc_create_dt  from t1
                left join aiai_dwh_dim_accounts_hist acc on t1.account = acc.account)
                select trans_id, card, trans_date, oper_type, amt, oper_result, terminal, t2.account, t2.crd_create_dt, t2.client, t2.acc_valid_to, t2.acc_create_dt, cl.last_name, cl.first_name, cl.patronymic, cl.date_of_birth, cl.passport_num, cl.passport_valid_to, cl.phone  from t2
                left join aiai_dwh_dim_clients_hist cl on t2.client = cl.client_id)
                select trans_id, card, trans_date, oper_type, amt, oper_result, t3.terminal, account, crd_create_dt, client, acc_valid_to, acc_create_dt, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, term.terminal_type, term.terminal_city, term.terminal_address from t3
                left join aiai_dwh_dim_terminals_hist term on t3.terminal = term.terminal_id)
                select * from t4;
              """
          )

    records_join = c.fetchall()
    names_join = [x[0] for x in c.description]
    join_table = pd.DataFrame(records_join, columns = names_join)

    c.execute(
        "select * from public.aiai_dwh_fact_passport_blacklist"
    )
    records_bl = c.fetchall()
    names_bl = [x[0] for x in c.description]
    bl = pd.DataFrame(records_bl, columns = names_bl)

    merged_table = join_table.merge(
    bl,
    how='left',
    on='passport_num'
)
join_table = join_table.drop_duplicates(subset='trans_id')

# Фильтрация строк, которые соответствуют условиям
mask = merged_table['entry_dt'].notnull() & (merged_table['trans_date'] >= merged_table['entry_dt'])

# Добавляем новый столбец или обновляем существующий
merged_table['event_type'] = None
merged_table.loc[mask, 'event_type'] = 'Заблокированный или просроченный паспорт'

join_table['event_type'] = merged_table['event_type']
join_table.loc[join_table['acc_valid_to'] < join_table['trans_date'], 'event_type'] = 'Просроченный договор'
join_table = join_table.sort_values(by=['client', 'trans_date'])
join_table['time_diff'] = join_table.groupby(['client', 'terminal_city'])['trans_date'].diff()
condition = (
    (join_table['time_diff'] < pd.Timedelta(hours=1)) &  # Разница меньше часа
    (join_table['terminal_city'] != join_table['terminal_city'].shift(1))  # Разные города
)
join_table.loc[condition, 'event_type'] = 'Совершение операций в разных городах за короткое время'
def detect_fraud(group):
    # Инициализация списка для отметки подозрительных транзакций
    fraud_indices = []
    # Скользящее окно для проверки условий
    for i in range(len(group) - 3):
        window = group.iloc[i:i + 4]  # Берем окно из 4 транзакций
        if (
            (window['trans_date'].max() - window['trans_date'].min() <= pd.Timedelta(minutes=20)) and  # Разница <= 20 минут
            all(window['amt'].diff().iloc[1:] < 0) and  # Каждая следующая сумма меньше предыдущей
            (window['oper_result'].iloc[-1] == 'success') and  # Последняя успешна
            all(window['oper_result'].iloc[:-1] == 'fail')  # Остальные неуспешны
        ):
            fraud_indices.extend(window.index)
    return fraud_indices

# Применяем функцию к каждой группе клиента

for client, group in join_table.groupby('client'):
    fraud_indices = detect_fraud(group)
    join_table.loc[fraud_indices, 'event_type'] = 'Попытка подбора суммы'

jt = join_table[join_table['event_type'].notna()].loc[:,['trans_date', 'passport_num', 'last_name', 'first_name', 'patronymic', 'phone', 'event_type']]
jt['report_dt'] = jt['trans_date'].dt.date
jt['fio'] = jt['patronymic'] + ' ' + jt['first_name'] + ' ' + jt['last_name'] 
jt = jt.rename(columns={"trans_date": "event_dt", "passport_num": "passport"})
report = jt[['event_dt', 'passport', 'fio', 'phone', 'event_type', 'report_dt']]
report = report.drop_duplicates()
report

with conn.cursor() as c:
    execute_values(
        cur=c,
        sql="""
                INSERT INTO public.aiai_rep_fraud (event_dt,passport,fio,phone,event_type,report_dt)
                values %s
            """,
        argslist = list(report.itertuples(index=False, name=None)) 
    ) 
conn.commit()
