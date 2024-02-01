"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import os
import csv

conn = psycopg2.connect(host="localhost", database="north", user="postgres", password="12345")
try:
    with conn:
        with conn.cursor() as cur:
            with open(os.path.relpath('homework-1/north_data/customers_data.csv', start='postgres-homeworks'), newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                for line in reader:
                    cur.execute('INSERT INTO customers VALUES (%s, %s, %s)', (line['customer_id'], line['company_name'], line['contact_name']))
            with open(os.path.relpath('homework-1/north_data/employees_data.csv', start='postgres-homeworks'), newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                for line in reader:
                    cur.execute('INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)', (line['employee_id'], line['first_name'], line['last_name'], line['title'], line['birth_date'], line['notes']))
            with open(os.path.relpath('homework-1/north_data/orders_data.csv', start='postgres-homeworks'), newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                for line in reader:
                    cur.execute('INSERT INTO orders VALUES (%s, %s, %s, %s, %s)', (line['order_id'], line['customer_id'], line['employee_id'], line['order_date'], line['ship_city']))
finally:
    conn.close()

