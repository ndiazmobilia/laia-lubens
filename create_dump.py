import sqlite3
import os

db_path = "output/data.db"
dump_path = "output/data_dump.sql"

# Ensure the output directory exists
os.makedirs(os.path.dirname(dump_path), exist_ok=True)

try:
    conn = sqlite3.connect(db_path)
    with open(dump_path, 'w', encoding="utf-8") as f:
        for line in conn.iterdump():
            f.write(f"{line}\n")
    conn.close()
    print(f"Successfully created SQL dump at {dump_path}")
except sqlite3.Error as e:
    print(f"Error creating SQL dump: {e}")
except FileNotFoundError:
    print(f"Error: Database file not found at {db_path}")
