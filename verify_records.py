import sqlite3

conn = sqlite3.connect('scd2.db')
cursor = conn.cursor()

cursor.execute('SELECT COUNT(*) FROM sales_records_cdc WHERE is_current = 1')
print('Active CDC records now:', cursor.fetchone()[0])

cursor.execute('SELECT id, product_name FROM sales_records_cdc WHERE id IN (8, 9) AND is_current = 1')
print('\nNew records:')
for row in cursor.fetchall():
    print(f'  ID {row[0]}: {row[1]}')

conn.close()
