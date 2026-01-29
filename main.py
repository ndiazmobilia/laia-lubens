import io
import pandas as pd
import sqlite3
from fastapi import FastAPI, HTTPException

import database_utils
import config
import gdrive

# Initialize the FastAPI application
app = FastAPI(
    title="Laia Lubens Data Loader",
    description="An API to load data from Google Drive into an in-memory SQL database.",
    version="1.0.0"
)

@app.get("/")
def read_root():
    """
    Root endpoint with a welcome message.
    """
    return {"message": "Welcome to the Data Loader API. Use /reload_[source_name] to load data."}

async def _process_drive_source(source_name: str, table_name: str, db_name: str, parse_func) -> dict:
    """
    Helper function to process a single Google Drive source.
    """
    source_url = config.DRIVE_SOURCES.get(source_name)
    if not source_url:
        return {
            "source": source_name,
            "success": False,
            "message": f"Configuration for '{source_name}' not found in environment variables."
        }

    service = gdrive.get_drive_service()
    if not service:
        return {
            "source": source_name,
            "success": False,
            "message": "Failed to authenticate with Google Drive. Check service_account.json."
        }

    folder_id = gdrive.extract_folder_id_from_url(source_url)
    if not folder_id:
        return {
            "source": source_name,
            "success": False,
            "message": "Invalid Google Drive folder URL."
        }

    files = gdrive.list_files_in_folder(service, folder_id)
    if not files:
        return {
            "source": source_name,
            "success": True,
            "message": f"No files found in the '{source_name}' directory."
        }

    processed_files_count = 0
    for file in files:
        print(f"Processing file: {file['name']} ({file['id']}) for source '{source_name}'")
        try:
            file_stream = gdrive.get_file_as_stream(service, file)
            if file_stream:
                parse_func(file_stream, db_name, table_name, file['name'])
                processed_files_count += 1
            else:
                print(f"Skipping file {file['name']} (unsupported format or empty stream).")
        except Exception as e:
            print(f"Could not process file {file['name']} for source '{source_name}': {e}")
            # Optionally, you might want to return an error for the whole source if any file fails
            # For now, we continue processing other files but log the error.

    if processed_files_count == 0:
        return {
            "source": source_name,
            "success": False,
            "message": f"Found files for '{source_name}', but none could be processed successfully."
        }

    return {
        "source": source_name,
        "success": True,
        "message": f"Successfully processed {processed_files_count} files and updated table '{table_name}'.",
        "processed_count": processed_files_count
    }

@app.post("/reload_all", tags=["Data Loading"])
async def reload_all():
    """
    Reloads data for all configured Google Drive sources.
    """
    results = []
    db_name = "output/data.db" # Consistent database name

    for source_name, source_url in config.DRIVE_SOURCES.items():
        table_name = source_name
        parse_func = database_utils.parse_html_to_db # Default parser

        if source_name == "citas" or source_name == "doctores":
            parse_func = database_utils.parse_xlsx_to_db

        result = await _process_drive_source(source_name, table_name, db_name, parse_func)
        results.append(result)

    return {"message": "Attempted to reload all sources.", "results": results}

@app.post("/reload_datos_personales", tags=["Data Loading"])
async def reload_datos_personales():
    """
    Reloads data for the 'datos_personales' source.
    """
    source_name = "datos_personales"
    table_name = "datos_personales"
    db_name = "output/data.db"
    result = await _process_drive_source(source_name, table_name, db_name, database_utils.parse_html_to_db)
    return result

@app.post("/reload_fechas_pacientes", tags=["Data Loading"])
async def reload_fechas_pacientes():
    """
    Reloads data for the 'fechas_pacientes' source.
    """
    source_name = "fechas_pacientes"
    table_name = "fechas_pacientes"
    db_name = "output/data.db"
    result = await _process_drive_source(source_name, table_name, db_name, database_utils.parse_html_to_db)
    return result

@app.post("/reload_facturas", tags=["Data Loading"])
async def reload_facturas():
    """
    Reloads data for the 'facturas' source.
    """
    source_name = "facturas"
    table_name = "facturas"
    db_name = "output/data.db"
    result = await _process_drive_source(source_name, table_name, db_name, database_utils.parse_html_to_db)
    return result

@app.post("/reload_cobros", tags=["Data Loading"])
async def reload_cobros():
    """
    Reloads data for the 'cobros' source.
    """
    source_name = "cobros"
    table_name = "cobros"
    db_name = "output/data.db"
    result = await _process_drive_source(source_name, table_name, db_name, database_utils.parse_html_to_db)
    return result

@app.post("/reload_citas", tags=["Data Loading"])
async def reload_citas():
    """
    Reloads data for the 'citas' source.
    """
    source_name = "citas"
    table_name = "citas"
    db_name = "output/data.db"
    result = await _process_drive_source(source_name, table_name, db_name, database_utils.parse_xlsx_to_db)
    return result

@app.post("/reload_doctores", tags=["Data Loading"])
async def reload_doctores():
    """
    Reloads data for the 'doctores' source.
    """
    source_name = "doctores"
    table_name = "doctores"
    db_name = "output/data.db"
    result = await _process_drive_source(source_name, table_name, db_name, database_utils.parse_xlsx_to_db)
    return result

@app.post("/reload_datos_tratamientos", tags=["Data Loading"])
async def reload_datos_tratamientos():
    """
    Reloads data for the 'datos_tratamientos' source.
    """
    source_name = "datos_tratamientos"
    table_name = "datos_tratamientos"
    db_name = "output/data.db"
    result = await _process_drive_source(source_name, table_name, db_name, database_utils.parse_html_to_db)
    return result

@app.post("/reload_trabajos_laboratorios", tags=["Data Loading"])
async def reload_trabajos_laboratorios():
    """
    Reloads data for the 'trabajos_laboratorios' source.
    """
    source_name = "trabajos_laboratorios"
    table_name = "trabajos_laboratorios"
    db_name = "output/data.db"
    result = await _process_drive_source(source_name, table_name, db_name, database_utils.parse_html_to_db)
    return result

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)