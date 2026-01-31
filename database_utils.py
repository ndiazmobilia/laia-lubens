import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime
import io

def parse_html_to_db(file_source, db_name, table_name, file_name="<stream>"):
    """
    Parses an HTML file (assumed to be an .xls file with HTML content),
    cleans the data, infers SQL types, creates/updates a table in an SQLite database,
    and populates it with data.

    Assumes:
    - The first data row contains column names.
    - The second data row is meaningless and should be ignored.
    - 'Código' column (if present) is used as a primary key for upsert operations.
    """
    try:
        tables = pd.read_html(file_source)
        if not tables:
            print(f"No tables found in the HTML file: {file_name}")
            return

        df = tables[0]

        # Set the first data row as column headers
        df.columns = df.iloc[0]

        # Drop the original first two rows (header row and meaningless second row)
        df = df.iloc[2:].reset_index(drop=True)

        # Clean column names for SQL (replace spaces, special chars)
        original_columns = df.columns.tolist()
        cleaned_columns = []
        for col in original_columns:
            clean_col = "".join(c for c in str(col) if c.isalnum() or c == '_').replace(' ', '_')
            cleaned_columns.append(clean_col)
        df.columns = cleaned_columns

        # Filter out columns named "nan"
        df = df.loc[:, df.columns != 'nan']

        # Specific modification for 'tratamientos' table
        if table_name == "tratamientos":
            if len(df.columns) > 0 and df.columns[0] == "Código":
                df.rename(columns={"Código": "CódigoPaciente"}, inplace=True)
                print(f"Renamed first column 'Código' to 'CódigoPaciente' for table '{table_name}'.")

        print(df.columns)
        # Connect to SQLite database
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        try:
            # Infer SQL types and create CREATE TABLE statement
            column_defs = []
            primary_key_col = None
            if 'Código' in df.columns: # Check for cleaned 'Código' column
                primary_key_col = 'Código'

            # Store inferred types for later use during insertion
            inferred_sql_types = {}

            for col_name, dtype in df.dtypes.items():
                sql_type = "TEXT" # Default

                if col_name == 'Código':
                    sql_type = "INTEGER"
                elif 'Fecha' in col_name:
                    sql_type = "DATETIME"
                elif "Importe" in col_name or "Saldo" in col_name or "Precio" in col_name or "Coste" in col_name: # Added for Importe/Saldo
                    sql_type = "REAL"
                elif 'object' in str(dtype):
                    # Try to infer more specific types for object columns
                    # Check if it can be converted to numeric (integer or real)
                    if pd.to_numeric(df[col_name], errors='coerce').notna().all():
                        # Check if all are integers
                        if (pd.to_numeric(df[col_name], errors='coerce') % 1 == 0).all():
                            sql_type = "INTEGER"
                        else:
                            sql_type = "REAL"
                    # Check if it can be converted to datetime
                    elif pd.to_datetime(df[col_name], errors='coerce').notna().all():
                        sql_type = "DATETIME"
                elif 'int' in str(dtype):
                    sql_type = "INTEGER"
                elif 'float' in str(dtype):
                    sql_type = "REAL"
                elif 'datetime' in str(dtype):
                    sql_type = "DATETIME"

                inferred_sql_types[col_name] = sql_type
                col_def = f'"{col_name}" {sql_type}'
                if col_name == primary_key_col:
                    col_def += " PRIMARY KEY"
                column_defs.append(col_def)

            create_table_sql = f'CREATE TABLE IF NOT EXISTS {table_name} ({", ".join(column_defs)})'
            # print(f"Created table sql '{create_table_sql}'")
            cursor.execute(create_table_sql)

            # Replace NaN with None for SQL NULL values
            df = df.replace({np.nan: None})

            # Prepare for insertion (INSERT OR REPLACE for upsert)
            cols = ', '.join([f'"{c}"' for c in df.columns])
            placeholders = ', '.join(['?' for _ in df.columns])
            insert_sql = f'INSERT OR REPLACE INTO {table_name} ({cols}) VALUES ({placeholders})'

            # Iterate and insert/update data with type conversion
            for index, row in df.iterrows():
                values = []
                for col_name, value in row.items():
                    # print(f'{col_name} = {value}')
                    sql_type = inferred_sql_types.get(col_name, "TEXT") # Get inferred type

                    if value is None:
                        values.append(None)
                    elif sql_type == "INTEGER":
                        try:
                            values.append(int(value))
                        except (ValueError, TypeError):
                            values.append(None)
                    elif sql_type == "REAL":
                        if isinstance(value, str):
                            if value.lower() == 'nan': # Handle "nan" string
                                values.append(None)
                            else:
                                try:
                                    # Replace comma with dot for decimal conversion
                                    cleaned_value = value.replace(',', '.')
                                    converted_value = float(cleaned_value)
                                    # Divide by 100 if the value is an integer and implies cents
                                    # This assumes the input is always an integer string representing cents
                                    # e.g., "1234" means 12.34
                                    if converted_value == int(converted_value): # Check if it's an integer float
                                        converted_value /= 100.0
                                    # print(converted_value)
                                    # values.append(converted_value)
                                    # print("{:.2f}".format(converted_value))
                                    values.append("{:.2f}".format(converted_value))
                                except (ValueError, TypeError):
                                    values.append(None)
                        else: # Not a string, try direct conversion
                            try:
                                converted_value = float(value)
                                if converted_value == int(converted_value): # Check if it's an integer float
                                    converted_value /= 100.0
                                values.append("{:.2f}".format(converted_value))
                            except (ValueError, TypeError):
                                values.append(None)
                    elif sql_type == "DATETIME":
                        try:
                            # Attempt to parse common date formats
                            if isinstance(value, str):
                                # Try dd/mm/yy first, then other common formats
                                try:
                                    dt_obj = datetime.strptime(value, '%d/%m/%y')
                                except ValueError:
                                    dt_obj = pd.to_datetime(value, dayfirst=True) # Let pandas handle other formats
                                values.append(dt_obj.isoformat())
                            else:
                                values.append(pd.to_datetime(value, dayfirst=True).isoformat())
                        except (ValueError, TypeError):
                            values.append(None)
                    else: # TEXT or other unhandled types
                        values.append(str(value))
                # print(f"{values}")
                cursor.execute(insert_sql, tuple(values))

            conn.commit()

            print(f"Successfully parsed '{file_name}' and populated table '{table_name}' in '{db_name}'.")

        finally:
            conn.close()

    except FileNotFoundError:
        print(f"Error: The file '{file_name}' was not found.")
    except Exception as e:
        print(f"An error occurred while parsing the HTML file or interacting with the database: {e}")


def parse_xlsx_to_db(file_stream, db_name, table_name, file_name="<stream>"):
    """
    Parses an XLSX file, cleans the data, infers SQL types, creates/updates a table
    in an SQLite database, and populates it with data.

    Assumes:
    - The first row of the XLSX file contains column names.
    - 'Código' column (if present) is used as a primary key for upsert operations.
    """
    try:
        # Read the XLSX file directly from the byte stream
        # file_stream.read() returns bytes, io.BytesIO wraps it into a file-like object
        df = pd.read_excel(io.BytesIO(file_stream.read()))
        # Clean column names for SQL (replace spaces, special chars)
        original_columns = df.columns.tolist()
        cleaned_columns = []
        for col in original_columns:
            clean_col = "".join(c for c in str(col) if c.isalnum() or c == '_').replace(' ', '_')
            cleaned_columns.append(clean_col)
        df.columns = cleaned_columns

        # Filter out columns named "nan" (can happen if there are empty columns in Excel)
        df = df.loc[:, df.columns != 'nan']

        # Connect to SQLite database
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        try:
            # Infer SQL types and create CREATE TABLE statement
            column_defs = []
            primary_key_col = None
            if 'Código' in df.columns: # Check for cleaned 'Código' column
                primary_key_col = 'Código'

            # Store inferred types for later use during insertion
            inferred_sql_types = {}

            for col_name, dtype in df.dtypes.items():
                sql_type = "TEXT" # Default
                if col_name == 'Código':
                    sql_type = "INTEGER"
                elif 'Fecha' in col_name:
                    sql_type = "DATETIME"
                elif "Importe" in col_name or "Saldo" in col_name or "Precio" in col_name or "Coste" in col_name:
                    sql_type = "REAL"
                elif 'object' in str(dtype):
                    # Try to infer more specific types for object columns
                    # Check if it can be converted to numeric (integer or real)
                    if pd.to_numeric(df[col_name], errors='coerce').notna().all():
                        # Check if all are integers
                        if (pd.to_numeric(df[col_name], errors='coerce') % 1 == 0).all():
                            sql_type = "INTEGER"
                        else:
                            sql_type = "REAL"
                    # Check if it can be converted to datetime
                    # elif pd.to_datetime(df[col_name], errors='coerce', dayfirst=True).notna().all():

                    # Check if it can be converted to datetime
                    elif pd.to_datetime(df[col_name], errors='coerce', dayfirst=True).notna().all():
                        sql_type = "DATETIME"
                elif 'int' in str(dtype):
                    sql_type = "INTEGER"
                elif 'float' in str(dtype):
                    sql_type = "REAL"
                elif 'datetime' in str(dtype):
                    sql_type = "DATETIME"

                inferred_sql_types[col_name] = sql_type
                col_def = f'"{col_name}" {sql_type}'
                if col_name == primary_key_col:
                    col_def += " PRIMARY KEY"
                column_defs.append(col_def)

            create_table_sql = f'CREATE TABLE IF NOT EXISTS {table_name} ({", ".join(column_defs)})'
            # print(f"Created table sql '{create_table_sql}'")
            cursor.execute(create_table_sql)

            # Replace NaN with None for SQL NULL values
            df = df.replace({np.nan: None})

            # Prepare for insertion (INSERT OR REPLACE for upsert)
            cols = ', '.join([f'"{c}"' for c in df.columns])
            placeholders = ', '.join(['?' for _ in df.columns])
            insert_sql = f'INSERT OR REPLACE INTO {table_name} ({cols}) VALUES ({placeholders})'

            # Iterate and insert/update data with type conversion
            for index, row in df.iterrows():
                values = []
                for col_name, value in row.items():
                    sql_type = inferred_sql_types.get(col_name, "TEXT")

                    if value is None:
                        values.append(None)
                    elif sql_type == "INTEGER":
                        try:
                            values.append(int(value))
                        except (ValueError, TypeError):
                            values.append(None)
                    elif sql_type == "REAL":
                        if isinstance(value, str):
                            if value.lower() == 'nan':
                                values.append(None)
                            else:
                                try:
                                    # Replace comma with dot for decimal conversion
                                    cleaned_value = value.replace(',', '.')
                                    converted_value = float(cleaned_value)
                                    # NOTE: This specific conversion (dividing by 100) is carried over
                                    # from parse_html_to_db. It assumes that integer-like float strings
                                    # represent cents and need to be converted to euros/dollars.
                                    # If 'citas' data does not follow this pattern, this logic might need adjustment.
                                    if converted_value == int(converted_value):
                                        converted_value /= 100.0
                                    values.append("{:.2f}".format(converted_value))
                                except (ValueError, TypeError):
                                    values.append(None)
                        else:
                            try:
                                converted_value = float(value)
                                if converted_value == int(converted_value):
                                    converted_value /= 100.0
                                values.append("{:.2f}".format(converted_value))
                            except (ValueError, TypeError):
                                values.append(None)
                    elif sql_type == "DATETIME":
                        try:

                            if isinstance(value, str):
                                # Attempt to parse common date formats
                                # Prioritize explicit formats to avoid inference issues
                                common_date_formats = ['%d/%m/%Y', '%d-%m-%Y', '%Y/%m/%d', '%Y-%m-%d', '%d/%m/%y', '%d-%m-%y']
                                parsed = False
                                for fmt in common_date_formats:
                                    try:
                                        dt_obj = datetime.strptime(value, fmt)
                                        values.append(dt_obj.isoformat())
                                        parsed = True
                                        break
                                    except ValueError:
                                        continue
                                if not parsed:
                                    # Fallback to pandas' robust parser if explicit formats fail
                                    dt_obj = pd.to_datetime(value, dayfirst=True, errors='coerce')
                                    if pd.notna(dt_obj):
                                        values.append(dt_obj.isoformat())
                                    else:
                                        values.append(None)
                            else:
                                dt_obj = pd.to_datetime(value, dayfirst=True, errors='coerce')
                                if pd.notna(dt_obj):
                                    values.append(dt_obj.isoformat())
                                else:
                                    values.append(None)
                        except (ValueError, TypeError):
                            values.append(None)
                    else:
                        values.append(str(value))
                cursor.execute(insert_sql, tuple(values))

            conn.commit()

            print(f"Successfully parsed '{file_name}' and populated table '{table_name}' in '{db_name}'.")

        finally:
            conn.close()

    except Exception as e:
        print(f"An error occurred while parsing the XLSX file or interacting with the database: {e}")

# Example usage (for testing, will be removed when integrated into main.py)
if __name__ == '__main__':
    # file_path = 'examples/DatosPersonales-01.01.2020-31.12.2025.xls'
    # file_path = 'examples/FechasPacientes-01.01.2023-31.12.2025.xls'
    # file_path = 'examples/Cobros-Todo2026.xls'
    # file_path = 'examples/Presupuestos-Todo-2026.xls'
    # file_path = 'examples/Doctoralia-informe_de_citas-01 - 28 feb 2025 (1).xlsx'
    # file_path = 'examples/exportarTratsExcel (18).xls'
    # file_path = 'examples/Doctores.xlsx'
    file_path = 'examples/Tratamientos_todo2026 2.xls'

    db_name = 'output/data.db'
    table_name = 'tratamientos'
    # For testing parse_html_to_db
    # with open('examples/DatosPersonales-01.01.2020-31.12.2025.xls', 'rb') as f:
    #     parse_html_to_db(f, db_name, 'personal_data_test')

    # For testing parse_xlsx_to_db
    with open(file_path, 'rb') as f:
        parse_html_to_db(f, db_name, table_name)
        # parse_xlsx_to_db(f, db_name, table_name)