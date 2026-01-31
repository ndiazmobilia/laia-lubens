import sqlite3
from datetime import datetime

def get_data_for_date_range(db_path: str, table_name: str, date_column: str, start_date: datetime, end_date: datetime) -> list[dict]:
    """
    Retrieves data from a specified table within a given date range.

    Args:
        db_path: The path to the SQLite database file.
        table_name: The name of the table to query.
        date_column: The name of the column containing date information.
        start_date: The start date (inclusive) for filtering.
        end_date: The end date (inclusive) for filtering.

    Returns:
        A list of dictionaries, where each dictionary represents a row of data.
    """
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row  # This allows accessing columns by name
        cursor = conn.cursor()

        # Convert datetime objects to ISO format strings for SQLite comparison
        start_date_str = start_date.isoformat()
        end_date_str = end_date.isoformat()

        query = f"SELECT * FROM {table_name} WHERE {date_column} BETWEEN ? AND ?"
        cursor.execute(query, (start_date_str, end_date_str))

        rows = cursor.fetchall()
        
        # Convert sqlite3.Row objects to dictionaries
        data = [dict(row) for row in rows]
        return data

    except sqlite3.Error as e:
        print(f"SQLite error in get_data_for_date_range: {e}")
        return []
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return []
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    DB_PATH = "output/data.db"
    
    # Example Usage:
    # Define your date range
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 12, 31)

    print(f"Retrieving data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

    # Example for 'citas' table
    # You need to know the actual date column name in your 'citas' table
    # For demonstration, let's assume it's 'Fecha'
    print("\n--- Data from 'citas' table ---")
    citas_data = get_data_for_date_range(DB_PATH, "citas", "Fecha", start_date, end_date)
    if citas_data:
        for row in citas_data[:5]: # Print first 5 rows as example
            print(row)
        print(f"... and {len(citas_data) - 5} more rows.")
    else:
        print("No data found for 'citas' in the specified date range.")

    # Example for 'doctores' table (assuming it also has a 'Fecha' column for some reason)
    # You would replace 'Fecha' with the actual date column name for doctors
    print("\n--- Data from 'doctores' table ---")
    doctores_data = get_data_for_date_range(DB_PATH, "doctores", "Fecha", start_date, end_date)
    if doctores_data:
        for row in doctores_data[:5]: # Print first 5 rows as example
            print(row)
        print(f"... and {len(doctores_data) - 5} more rows.")
    else:
        print("No data found for 'doctores' in the specified date range.")

    # Add more examples for other tables as needed
