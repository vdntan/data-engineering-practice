import psycopg2
import csv
from datetime import datetime

def create_tables(cur):
    with open("schema.sql", "r") as f:
        cur.execute(f.read())

def insert_csv_data(cur, conn):
    # Insert accounts.csv
    with open("data/accounts.csv", "r") as f:
        reader = csv.DictReader(f, skipinitialspace=True)
        for row in reader:
            cur.execute("""
                INSERT INTO accounts VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                int(row['customer_id']), row['first_name'], row['last_name'],
                row['address_1'], row['address_2'], row['city'],
                row['state'], row['zip_code'],
                datetime.strptime(row['join_date'], '%Y/%m/%d').date()
            ))

    # Insert products.csv
    with open("data/products.csv", "r") as f:
        reader = csv.DictReader(f, skipinitialspace=True)
        for row in reader:
            cur.execute("""
                INSERT INTO products VALUES (%s, %s, %s)
            """, (int(row['product_id']), row['product_code'], row['product_description']))

    # Insert transactions.csv
    with open("data/transactions.csv", "r") as f:
        reader = csv.DictReader(f, skipinitialspace=True)
        for row in reader:
            cur.execute("""
                INSERT INTO transactions VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                row['transaction_id'],
                datetime.strptime(row['transaction_date'], '%Y/%m/%d').date(),
                int(row['product_id']),
                row['product_code'],
                row['product_description'],
                int(row['quantity']),
                int(row['account_id'])
            ))

    conn.commit()

def main():
    conn = psycopg2.connect(
        host="postgres",
        database="postgres",
        user="postgres",
        password="postgres"
    )
    cur = conn.cursor()
    create_tables(cur)
    insert_csv_data(cur, conn)
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
