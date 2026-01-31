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
        print(f"SQLite error in get_data_for_date_range (Table: {table_name}, Column: {date_column}): {e}")
        return []
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return []
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    DB_PATH = "output/data.db"
    
    # Define your date range (example: entire year 2023)
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 12, 31, 23, 59, 59) # End of day for inclusivity

    print(f"--- Daily Check Data Retrieval ---")
    print(f"Date Range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

    # Define tables and their corresponding date columns for filtering
    tables_to_check = {
        "citas": "Fecha",
        "cobros": "Fechadecobro",
        "datos_personales": "Fechadenacimiento",
        "facturas": "Fechafacturado",
        "fechas_pacientes": "Fechaprimeravisita",
        "presupuestos": "Fechacreación",
        "tratamientos": "Fecharealizado",
    }

    all_retrieved_data = {}

    for table_name, date_column in tables_to_check.items():
        print(f"\n--- Retrieving data for '{table_name}' (filtered by '{date_column}') ---")
        data = get_data_for_date_range(DB_PATH, table_name, date_column, start_date, end_date)
        all_retrieved_data[table_name] = data

        if data:
            print(f"Found {len(data)} records.")
            print("First 5 records (or fewer if less than 5):")
            for row in data[:5]:
                print(row)
        else:
            print(f"No data found for '{table_name}' in the specified date range or an error occurred.")

    print("\n--- Summary of Retrieved Data ---")
    for table_name, data in all_retrieved_data.items():
        print(f"'{table_name}': {len(data)} records")

    # You can now process 'all_retrieved_data' further as needed.
    # For example, to access citas data:
    # citas_data = all_retrieved_data.get("citas", [])
    # for cita in citas_data:
    #     print(f"Cita: {cita.get('Paciente')} on {cita.get('Fecha')}")