import sqlite3

db_name = 'output/data.db'
table_name = 'datos_tratamientos'

try:
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    print(f"--- First 3 rows from '{table_name}' table ---")
    cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
    rows = cursor.fetchall()
    
    # Print column names
    column_names = [description[0] for description in cursor.description]
    print(column_names)

    for row in rows:
        print(row)

    print(f"\n--- CREATE TABLE statement for '{table_name}' ---")
    cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}'")
    create_table_sql = cursor.fetchone()
    if create_table_sql:
        print(create_table_sql[0])
    else:
        print(f"Table '{table_name}' not found in the database.")

    conn.close()

except sqlite3.Error as e:
    print(f"SQLite error: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
