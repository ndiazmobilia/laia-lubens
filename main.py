import io
import pandas as pd
import sqlite3 # Added for direct SQLite connection
from fastapi import FastAPI, HTTPException
# from sqlalchemy.exc import SQLAlchemyError # Not needed if not using SQLAlchemy engine

import database_utils
import config
import database
import gdrive

# Initialize the FastAPI application
app = FastAPI(
    title="Laia Lubens Data Loader",
    description="An API to load data from Google Drive into an in-memory SQL database.",
    version="1.0.0"
)

# Get the shared database engine
# db_engine = database.get_db_engine() # This is no longer needed as database_utils handles SQLite connection


@app.get("/")
def read_root():
    """
    Root endpoint with a welcome message.
    """
    return {"message": "Welcome to the Data Loader API. Use /reload_[source_name] to load data."}


@app.post("/reload_datos_personales", tags=["Data Loading"])
def reload_datos_personales():
    """
    Reloads data for the 'datos_personales' source.

    This endpoint fetches all Excel files from the Google Drive directory specified
    in the DATOS_PERSONALES_GOOGLE_DRIVE_DIR environment variable, processes them,
    loads them into an in-memory SQLite table named 'datos_personales', and
    returns a success message.
    """
    source_name = "datos_personales"
    table_name = "personal_data" # Using the table name from database_utils
    db_name = "output/data.db" # Using the database name from database_utils

    # 1. Get Drive URL from config
    source_url = config.DRIVE_SOURCES.get(source_name)
    if not source_url:
        raise HTTPException(
            status_code=404,
            detail=f"Configuration for '{source_name}' not found in environment variables."
        )

    # 2. Get authenticated Google Drive service
    service = gdrive.get_drive_service()
    if not service:
        raise HTTPException(
            status_code=500,
            detail="Failed to authenticate with Google Drive. Check service_account.json."
        )

    # 3. Extract Folder ID and list files
    folder_id = gdrive.extract_folder_id_from_url(source_url)
    if not folder_id:
        raise HTTPException(status_code=400, detail="Invalid Google Drive folder URL.")

    files = gdrive.list_files_in_folder(service, folder_id)
    if not files:
        return {"message": f"No files found in the '{source_name}' directory.", "success": False}

    # 4. Download and process each file
    processed_files_count = 0
    for file in files:
        print(f"Processing file: {file['name']} ({file['id']})")
        try:
            file_stream = gdrive.get_file_as_stream(service, file)

            if file_stream:
                # Use the new utility function to parse HTML-like XLS files and populate the DB
                database_utils.parse_html_to_db(file_stream, db_name, table_name, file['name'])
                processed_files_count += 1
            else:
                # The file was skipped (e.g., unsupported format)
                continue

        except Exception as e:
            print(f"Could not process file {file['name']}: {e}")
            continue

    if processed_files_count == 0:
        return {"message": "Found files, but none could be processed successfully.", "success": False}

    # 5. Query last 10 records from the newly populated SQLite database and return as confirmation
    # The preview is no longer needed, so this block can be removed or simplified.
    # For now, I'll keep the try-except for sqlite3.connect but remove the result_df and result_json.
    try:
        with sqlite3.connect(db_name) as conn:
            # No need to query for preview records
            pass
    except sqlite3.Error as e:
        raise HTTPException(status_code=500, detail=f"Failed to connect to database: {e}")

    return {
        "message": f"Successfully processed {processed_files_count} files and updated table '{table_name}'.",
        "success": True
    }


@app.post("/reload_fechas_pacientes", tags=["Data Loading"])
def reload_fechas_pacientes():
    """
    Reloads data for the 'fechas_pacientes' source.

    This endpoint fetches all Excel files from the Google Drive directory specified
    in the FECHAS_PACIENTES_GOOGLE_DRIVE_DIR environment variable, processes them,
    loads them into an in-memory SQLite table named 'fechas_pacientes', and
    returns a success message.
    """
    source_name = "fechas_pacientes"
    table_name = "fechas_pacientes"
    db_name = "output/data.db"

    # 1. Get Drive URL from config
    source_url = config.DRIVE_SOURCES.get(source_name)
    if not source_url:
        raise HTTPException(
            status_code=404,
            detail=f"Configuration for '{source_name}' not found in environment variables."
        )

    # 2. Get authenticated Google Drive service
    service = gdrive.get_drive_service()
    if not service:
        raise HTTPException(
            status_code=500,
            detail="Failed to authenticate with Google Drive. Check service_account.json."
        )

    # 3. Extract Folder ID and list files
    folder_id = gdrive.extract_folder_id_from_url(source_url)
    if not folder_id:
        raise HTTPException(status_code=400, detail="Invalid Google Drive folder URL.")

    files = gdrive.list_files_in_folder(service, folder_id)
    if not files:
        return {"message": f"No files found in the '{source_name}' directory.", "success": False}

    # 4. Download and process each file
    processed_files_count = 0
    for file in files:
        print(f"Processing file: {file['name']} ({file['id']})")
        try:
            file_stream = gdrive.get_file_as_stream(service, file)

            if file_stream:
                # Use the new utility function to parse HTML-like XLS files and populate the DB
                database_utils.parse_html_to_db(file_stream, db_name, table_name, file['name'])
                processed_files_count += 1
            else:
                # The file was skipped (e.g., unsupported format)
                continue

        except Exception as e:
            print(f"Could not process file {file['name']}: {e}")
            continue

    if processed_files_count == 0:
        return {"message": "Found files, but none could be processed successfully.", "success": False}

    # 5. Query last 10 records from the newly populated SQLite database and return as confirmation
    try:
        with sqlite3.connect(db_name) as conn:
            # No need to query for preview records
            pass
    except sqlite3.Error as e:
        raise HTTPException(status_code=500, detail=f"Failed to connect to database: {e}")

    return {
        "message": f"Successfully processed {processed_files_count} files and updated table '{table_name}'.",
        "success": True
    }




@app.post("/reload_facturas", tags=["Data Loading"])
def reload_facturas():
    """
    Reloads data for the 'facturas' source.

    This endpoint fetches all Excel files from the Google Drive directory specified
    in the FACTURAS_GOOGLE_DRIVE_DIR environment variable, processes them,
    loads them into an in-memory SQLite table named 'facturas', and
    returns a success message.
    """
    source_name = "facturas"
    table_name = "facturas"
    db_name = "output/data.db"

    # 1. Get Drive URL from config
    source_url = config.DRIVE_SOURCES.get(source_name)
    if not source_url:
        raise HTTPException(
            status_code=404,
            detail=f"Configuration for '{source_name}' not found in environment variables."
        )

    # 2. Get authenticated Google Drive service
    service = gdrive.get_drive_service()
    if not service:
        raise HTTPException(
            status_code=500,
            detail="Failed to authenticate with Google Drive. Check service_account.json."
        )

    # 3. Extract Folder ID and list files
    folder_id = gdrive.extract_folder_id_from_url(source_url)
    if not folder_id:
        raise HTTPException(status_code=400, detail="Invalid Google Drive folder URL.")

    files = gdrive.list_files_in_folder(service, folder_id)
    if not files:
        return {"message": f"No files found in the '{source_name}' directory.", "success": False}

    # 4. Download and process each file
    processed_files_count = 0
    for file in files:
        print(f"Processing file: {file['name']} ({file['id']})")
        try:
            file_stream = gdrive.get_file_as_stream(service, file)

            if file_stream:
                # Use the new utility function to parse HTML-like XLS files and populate the DB
                database_utils.parse_html_to_db(file_stream, db_name, table_name, file['name'])
                processed_files_count += 1
            else:
                # The file was skipped (e.g., unsupported format)
                continue

        except Exception as e:
            print(f"Could not process file {file['name']}: {e}")
            continue

    if processed_files_count == 0:
        return {"message": "Found files, but none could be processed successfully.", "success": False}

    # 5. Query last 10 records from the newly populated SQLite database and return as confirmation
    try:
        with sqlite3.connect(db_name) as conn:
            # No need to query for preview records
            pass
    except sqlite3.Error as e:
        raise HTTPException(status_code=500, detail=f"Failed to connect to database: {e}")

    return {
        "message": f"Successfully processed {processed_files_count} files and updated table '{table_name}'.",
        "success": True
    }


@app.post("/reload_cobros", tags=["Data Loading"])
def reload_cobros():
    """
    Reloads data for the 'cobros' source.

    This endpoint fetches all Excel files from the Google Drive directory specified
    in the COBROS_GOOGLE_DRIVE_DIR environment variable, processes them,
    loads them into an in-memory SQLite table named 'cobros', and
    returns a success message.
    """
    source_name = "cobros"
    table_name = "cobros"
    db_name = "output/data.db"

    # 1. Get Drive URL from config
    source_url = config.DRIVE_SOURCES.get(source_name)
    if not source_url:
        raise HTTPException(
            status_code=404,
            detail=f"Configuration for '{source_name}' not found in environment variables."
        )

    # 2. Get authenticated Google Drive service
    service = gdrive.get_drive_service()
    if not service:
        raise HTTPException(
            status_code=500,
            detail="Failed to authenticate with Google Drive. Check service_account.json."
        )

    # 3. Extract Folder ID and list files
    folder_id = gdrive.extract_folder_id_from_url(source_url)
    if not folder_id:
        raise HTTPException(status_code=400, detail="Invalid Google Drive folder URL.")

    files = gdrive.list_files_in_folder(service, folder_id)
    if not files:
        return {"message": f"No files found in the '{source_name}' directory.", "success": False}

    # 4. Download and process each file
    processed_files_count = 0
    for file in files:
        print(f"Processing file: {file['name']} ({file['id']})")
        try:
            file_stream = gdrive.get_file_as_stream(service, file)

            if file_stream:
                # Use the new utility function to parse HTML-like XLS files and populate the DB
                database_utils.parse_html_to_db(file_stream, db_name, table_name, file['name'])
                processed_files_count += 1
            else:
                # The file was skipped (e.g., unsupported format)
                continue

        except Exception as e:
            print(f"Could not process file {file['name']}: {e}")
            continue

    if processed_files_count == 0:
        return {"message": "Found files, but none could be processed successfully.", "success": False}

    # 5. Query last 10 records from the newly populated SQLite database and return as confirmation
    try:
        with sqlite3.connect(db_name) as conn:
            # No need to query for preview records
            pass
    except sqlite3.Error as e:
        raise HTTPException(status_code=500, detail=f"Failed to connect to database: {e}")

    return {
        "message": f"Successfully processed {processed_files_count} files and updated table '{table_name}'.",
        "success": True
    }


@app.post("/reload_citas", tags=["Data Loading"])
def reload_citas():
    """
    Reloads data for the 'citas' source.

    This endpoint fetches all XLSX files from the Google Drive directory specified
    in the CITAS_GOOGLE_DRIVE_DIR environment variable, processes them,
    loads them into an in-memory SQLite table named 'citas', and
    returns a success message.
    """
    source_name = "citas"
    table_name = "citas"
    db_name = "output/data.db" # Ensure this is consistent with other endpoints

    # 1. Get Drive URL from config
    source_url = config.DRIVE_SOURCES.get(source_name)
    if not source_url:
        raise HTTPException(
            status_code=404,
            detail=f"Configuration for '{source_name}' not found in environment variables."
        )

    # 2. Get authenticated Google Drive service
    if not service:
        raise HTTPException(
            status_code=500,
            detail="Failed to authenticate with Google Drive. Check service_account.json."
        )

    # 3. Extract Folder ID and list files
    folder_id = gdrive.extract_folder_id_from_url(source_url)
    if not folder_id:
        raise HTTPException(status_code=400, detail="Invalid Google Drive folder URL.")

    files = gdrive.list_files_in_folder(service, folder_id)
    if not files:
        return {"message": f"No files found in the '{source_name}' directory.", "success": False}

    # 4. Download and process each file
    processed_files_count = 0
    for file in files:
        print(f"Processing file: {file['name']} ({file['id']})")
        try:
            file_stream = gdrive.get_file_as_stream(service, file)

            if file_stream:
                # Use the new utility function to parse XLSX files and populate the DB
                database_utils.parse_xlsx_to_db(file_stream, db_name, table_name, file['name'])
                processed_files_count += 1
            else:
                # The file was skipped (e.g., unsupported format)
                continue

        except Exception as e:
            print(f"Could not process file {file['name']}: {e}")
            continue

    if processed_files_count == 0:
        return {"message": "Found files, but none could be processed successfully.", "success": False}

    # 5. Query last 10 records from the newly populated SQLite database and return as confirmation
    try:
        with sqlite3.connect(db_name) as conn:
            # No need to query for preview records
            pass
    except sqlite3.Error as e:
        raise HTTPException(status_code=500, detail=f"Failed to connect to database: {e}")

    return {
        "message": f"Successfully processed {processed_files_count} files and updated table '{table_name}'.",
        "success": True
    }


if __name__ == "__main__":
    import uvicorn
    # To run this directly for testing:
    # uvicorn main:app --reload
    uvicorn.run(app, host="0.0.0.0", port=8000)