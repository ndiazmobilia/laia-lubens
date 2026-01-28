from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
import pandas as pd

# In-memory SQLite database
# The 'check_same_thread=False' is important for FastAPI usage.
DATABASE_URL = "sqlite:///:memory:"

# The connect_args are necessary to allow the same connection to be used across different threads,
# which is what happens when a new request comes into a FastAPI application.
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})


def get_db_engine() -> Engine:
    """
    Returns the shared database engine instance.
    """
    return engine


def  load_dataframe_to_db(df: pd.DataFrame, table_name: str, engine: Engine):
    """
    Loads a pandas DataFrame into a specified table in the database.
    If the table already exists, it will be replaced.

    :param df: The pandas DataFrame to load.
    :param table_name: The name of the SQL table to create/replace.
    :param engine: The SQLAlchemy engine to use for the connection.
    """
    try:
        # The 'if_exists="replace"' parameter handles dropping and recreating the table.
        # 'index=False' prevents pandas from writing the DataFrame index as a column.
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        print(f"Successfully loaded DataFrame into table '{table_name}'.")
    except Exception as e:
        print(f"An error occurred while loading data into the database: {e}")

