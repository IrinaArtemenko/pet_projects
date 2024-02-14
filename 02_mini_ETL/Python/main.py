import csv
import time
from datetime import datetime
import psycopg2
from config import balance_csv, balance_tbl, posting_csv, posting_tbl, account_csv, account_tbl, currency_csv,\
    currency_tbl, exchange_rate_csv, ledger_account_csv, exchange_rate_tbl, ledger_account_tbl

action1 = 'start download'
action2 = 'end download'


def load_table(csv_name, tbl_name):
    conn = psycopg2.connect(dbname='postgres', user='postgres', password='123', host='localhost')
    cur = conn.cursor()
    with open(csv_name) as f:
        reader = csv.reader(f)
        a = len(list(reader)[0])
        b = "%s, " * a
        c = b[:-2]
    with open(csv_name, 'r', encoding='cp866') as f:
        reader = csv.reader(f)
        next(reader)
        sql1 = "DELETE FROM " + tbl_name
        cur.execute("INSERT INTO logs.t_logs_download(load_dtm, target_table, action) VALUES (%s, %s, %s)",\
                    (datetime.now(), tbl_name, action1))
        cur.execute(sql1)
        time.sleep(5)
        sql2 = "INSERT INTO " + tbl_name + " VALUES(" + c + ")"
        for row in reader:
            cur.execute(sql2, row)
        cur.execute("INSERT INTO logs.t_logs_download(load_dtm, target_table, action) VALUES (%s, %s, %s)",\
                    (datetime.now(), tbl_name, action2))
    conn.commit()
    cur.close()
    conn.close()


if __name__ == '__main__':
    load_table(balance_csv, balance_tbl)
    load_table(posting_csv, posting_tbl)
    load_table(account_csv, account_tbl)
    load_table(currency_csv, currency_tbl)
    load_table(exchange_rate_csv, exchange_rate_tbl)
    load_table(ledger_account_csv, ledger_account_tbl)



