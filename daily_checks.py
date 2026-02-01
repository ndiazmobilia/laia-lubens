import difflib
import logging
import sqlite3
from collections import defaultdict
from datetime import datetime
from typing import List, Dict, Any

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s - %(asctime)s - %(module)s - %(funcName)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Doctor numbers to names mapping
doctors = {
    "15": "Anna Pevrukhina",
    "14": "Juan Millet",
    "11": "Alejandro Cordero",
    "2": "Agusti Ferrando i Estrella",
    "18": "Claudia Degens",
    "22": "Dayana Arizaka Riquelme",
    "21": "Anna Shchilova",
    "10": "Macarena Remohi Martínez-Medina",
    "20": "Melissa Rivera",
    "23": "Carmen Herrero",
    "7": "María Florencia Cerviche",
    "24": "Clara Garcia Saiz",
    "25": "Macarena Ramirez Nunez",
    "26": "Maria Contreras Miquel",
    "8": "Beatriz Marquez Garcia",
    "6": "Abigail Cevallos Madrid",
    "27": "Julia Romanenko",
    "28": "Celia Gil Manzanero",
    "29": "Marta Bravo Diaz",
    "30": "Victoria Arocena Fernandez",
    "31": "Raul Miguel Biot",
    "32": "Julio Garcia Algarra",
    "33": "Andrea Vicente Pardo"
}


def get_data_for_date_range(db_path: str, table_name: str, date_column: str, start_date: datetime,
                            end_date: datetime) -> list[dict]:
    """
    Retrieves data from a specified table within a given date range,
    joining with the datos_personales table to include patient name information.

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
        # Determine the join key based on the table name
        join_key = "Código"  # Default join key
        if table_name == "tratamientos":
            join_key = "CódigoPaciente"
            # Construct the query with a LEFT JOIN
            query = f"""
                SELECT t.*, dp.Nombre, dp.Apellido1, dp.Apellido2
                FROM {table_name} t
                LEFT JOIN datos_personales dp ON t.{join_key} = dp.Código
                WHERE t.{date_column} BETWEEN ? AND ?
            """

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


def get_treatments(db_path: str, start_date: datetime,
                            end_date: datetime) -> list[dict]:
    """
    Retrieves data from a specified table within a given date range.

    Args:
        db_path: The path to the SQLite database file.
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

        query = f"SELECT * FROM tratamientos WHERE 'Fecharealizado' BETWEEN ? AND ?"
        cursor.execute(query, (start_date_str, end_date_str))
        # Nombre']} {entry['Apellido 1']} {entry['Apellido 2'
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


def perform_appointment_checks(from_date, to_date):
    appointments = get_data_for_date_range("output/data.db", "citas", "Fecha", from_date, to_date)
    treatments = get_data_for_date_range("output/data.db", "tratamientos", "Fecharealizado", from_date, to_date)

    appointments = [
        appointment for appointment in appointments
        if appointment['Paciente'] not in (None, '')
    ]

    # Normalize dates in doctoralia_appointments
    for appt in appointments:
        appt['Fecha_normalizada'] = normalize_date(appt['Fecha'], '%Y-%m-%dT%H:%M:%S')
    # Normalize dates in cliniwin_treatments
    for treatment in treatments:
        treatment['Fecha_normalizada'] = normalize_date(treatment['Fecharealizado'], '%Y-%m-%dT%H:%M:%S')

    # Step 2: Group data by normalized date
    grouped_appointments = defaultdict(list)
    grouped_treatments = defaultdict(list)

    for appt in appointments:
        if appt['Fecha_normalizada']:
            grouped_appointments[appt['Fecha_normalizada']].append(appt)

    for treatment in treatments:
        if treatment['Fecha_normalizada']:
            grouped_treatments[treatment['Fecha_normalizada']].append(treatment)

    alerts = []
    # Iterate through dates and compare
    # print(grouped_appointments.keys())
    all_dates = set(grouped_appointments.keys()).union(grouped_treatments.keys())

    for date in all_dates:
        appointments = grouped_appointments.get(date, [])
        treatments = grouped_treatments.get(date, [])
        alerts.extend(perform_checks(appointments, treatments))
    return alerts


def perform_checks(appointments, treatments):
    alerts = []
    state_alerts = check_state(appointments)
    treatment_alerts = check_treatments(appointments, treatments)
    doctors_alerts = check_doctors(appointments, treatments)
    alerts.extend(state_alerts)
    alerts.extend(treatment_alerts)
    alerts.extend(doctors_alerts)
    # print(alerts)
    return alerts


def check_state(appointments):
    # print("Checking state...")
    allowed_statuses = {'Visita realizada', 'No ha venido', 'Cancelada'}
    alerts = []
    # print(appointments)
    # Check `Estado` in doctoralia_appointments
    for appointment in appointments:
        if appointment['Estado'] not in allowed_statuses:
            alerts.append({
                "type": "Estado invalido",
                "message": f"El paciente {appointment['Paciente']} con fecha {appointment['Fecha']} no tiene estado en Visita realizada, No ha venido, o Cancelada",
                "data": appointment
            })
    # print(alerts)
    return alerts


def normalize_name(name: str) -> str:
    """Normalize a name by converting to lowercase and removing extra spaces."""
    return " ".join(name.lower().strip().split())


def generate_suggestions(target_name: str, candidates: List[str]) -> List[str]:
    """Generate a list of suggested names using difflib for close matches."""
    return difflib.get_close_matches(target_name, candidates, n=3, cutoff=0.5)


def check_treatments(appointments: List[Dict[str, Any]], treatments: List[Dict[str, Any]]) -> list[
    dict[str, str | dict[str, Any]]]:
    # print("Checking treatments...")
    alerts = []

    # Build a list of normalized names from cliniwin_treatments
    cliniwin_names = []
    for entry in treatments:
        # Prioritize name from datos_personales, fallback to original name fields
        if entry.get('Nombre') and entry.get('Apellido1'):
            name = f"{entry['Nombre']} {entry['Apellido1']} {entry.get('Apellido2', '')}".strip()
        else:
            # Fallback to the original name fields if the join failed
            name = f"{entry.get('Nombre', '')} {entry.get('Apellido 1', '')} {entry.get('Apellido 2', '')}".strip()
        cliniwin_names.append(normalize_name(name))


    for appointment in appointments:
        # Process only appointments with 'Estado' == 'Visita realizada'
        if appointment['Estado'] != 'Visita realizada':
            continue

        # Prioritize name from datos_personales, fallback to original 'Paciente' field
        if appointment.get('Nombre') and appointment.get('Apellido1'):
            patient_name_str = f"{appointment['Nombre']} {appointment['Apellido1']} {appointment.get('Apellido2', '')}".strip()
        else:
            patient_name_str = appointment['Paciente']

        patient_name = normalize_name(patient_name_str)

        # Check if the patient is in cliniwin_treatments
        if patient_name not in cliniwin_names:
            suggestions = generate_suggestions(patient_name, cliniwin_names)
            alerts.append({
                "type": "Tratamiento inexistente",
                "message": f"El paciente {patient_name_str} con fecha {appointment['Fecha']} no tiene un tratamiento realizado en Cliniwin. Sugerencias: {suggestions or 'Ninguna'}",
                "data": appointment
            })
    # print(alerts)
    return alerts


def match_patient_name(patient_name: str, cliniwin_names: List[str]) -> str:
    """Find the best match for a patient's name using difflib."""
    matches = difflib.get_close_matches(patient_name, cliniwin_names, n=1, cutoff=0.5)
    return matches[0] if matches else None


def check_doctors(
        appointments: List[Dict[str, str]],
        treatments: List[Dict[str, str]]
) -> list[dict[str, str | dict[str, str]]]:
    # print("Checking doctors...")
    alerts = []
    # logging.info(f'this is the treatments: {treatments}')
    # logging.info(f'this is the appointments: {appointments}')

    # Create a mapping of normalized names to cliniwin treatments
    cliniwin_mapping = {}
    for entry in treatments:
        if entry.get('Nombre') and entry.get('Apellido1'):
            name = f"{entry['Nombre']} {entry['Apellido1']} {entry.get('Apellido2', '')}".strip()
        else:
            name = f"{entry.get('Nombre', '')} {entry.get('Apellido 1', '')} {entry.get('Apellido 2', '')}".strip()
        cliniwin_mapping[normalize_name(name)] = entry

    # logging.info(f'this is the cliniwin mapping: {cliniwin_mapping}')
    for appointment in appointments:
        if appointment['Estado'] != 'Visita realizada':
            continue

        # Normalize the patient name from Doctoralia
        if appointment.get('Nombre') and appointment.get('Apellido1'):
            patient_name_str = f"{appointment['Nombre']} {appointment['Apellido1']} {appointment.get('Apellido2', '')}".strip()
        else:
            patient_name_str = appointment['Paciente']
        patient_name = normalize_name(patient_name_str)


        # Find the best match for the patient in Cliniwin
        matched_name = match_patient_name(patient_name, list(cliniwin_mapping.keys()))

        if not matched_name:
            continue

        cliniwin_entry = cliniwin_mapping[matched_name]
        # Compare doctor numbers
        doctoralia_doctor_name = appointment['Especialista']
        doctoralia_doctor_number = [key for key, value in doctors.items() if value == doctoralia_doctor_name]
        doctor_en_cliniwin = cliniwin_entry['NumDoctor']
        # que no sea el doctor 18 que es claudia
        if doctoralia_doctor_number and doctoralia_doctor_number[
            0] != doctor_en_cliniwin and doctor_en_cliniwin != '18':
            cliniwin_doctor_name = doctors.get(doctor_en_cliniwin, "Desconocido")
            alerts.append({
                "type": "Doctor erroneo",
                "message": f"Doctor no coincide para el paciente {patient_name_str}: Doctoralia ({doctoralia_doctor_name}) vs. Cliniwin ({cliniwin_doctor_name}).",
                "data": appointment
            })
    # print(alerts)
    return alerts


# Step 1: Normalize the dates to ISO 8601 format
def normalize_date(date_str, input_format, output_format='%d-%m-%Y'):
    try:
        return datetime.strptime(date_str, input_format).strftime(output_format)
    except ValueError:
        return None


doctoralia_appointments = [
    {'Fecha': '11/11/2024', 'Paciente': 'Antón Kolesnik', 'Especialista': 'Anna Pevrukhina',
     'Servicios': 'Primera visita Odontología', 'Aseguradora': 'Sin aseguradora', 'Precio': '',
     'Origen de la cita': 'Agenda', 'Estado': 'Visita realizada', 'Creación de la cita': '05/11/2024'},
    {'Fecha': '11/11/2024', 'Paciente': 'VITALI STEPANENKO', 'Especialista': 'Alejandro Cordero',
     'Servicios': 'Primera visita Odontología', 'Aseguradora': '', 'Precio': '', 'Origen de la cita': 'Agenda',
     'Estado': 'Visita realizada', 'Creación de la cita': '21/10/2024'},
    {'Fecha': '11/11/2024', 'Paciente': 'VITALI STEPANENKO 2', 'Especialista': 'Anna Pevrukhina',
     'Servicios': 'Primera visita Odontología', 'Aseguradora': '', 'Precio': '', 'Origen de la cita': 'Agenda',
     'Estado': 'Cancelada', 'Creación de la cita': '21/10/2024'},
    {'Fecha': '11/11/2024', 'Paciente': 'VITALI STEPANENKO 3', 'Especialista': 'Anna Pevrukhina',
     'Servicios': 'Primera visita Odontología', 'Aseguradora': '', 'Precio': '', 'Origen de la cita': 'Agenda',
     'Estado': 'Programada', 'Creación de la cita': '21/10/2024'}
]

cliniwin_treatments = [
    {'Fecha realizado': '11/11/24', 'Nombre': 'VITALI', 'Apellido 1': 'STEPANENKO', 'Apellido 2': '', 'Dni': 'Y778163P',
     'Móvil': '651559048', 'Cómo nos ha conocido': 'Referido Anna U', 'Descripción': 'PRIMERA VISITA ODONTOLOGIA',
     'Doctor': 'ANNA', 'Coste': '0,00', 'Importe': '0,00', 'Fecha última visita': '11/11/24', 'Fecha próxima cita': '',
     'Num. Doctor': '15'},
    {'Fecha realizado': '11/11/24', 'Nombre': 'Vitalii', 'Apellido 1': 'Stepanenko', 'Apellido 2': '',
     'Dni': 'FY026540', 'Móvil': '', 'Cómo nos ha conocido': 'Referido Anna U', 'Descripción': 'RECONSTRUCCIÓN',
     'Doctor': 'ANNA', 'Coste': '0,00', 'Importe': '80,00', 'Fecha última visita': '11/11/24', 'Fecha próxima cita': '',
     'Num. Doctor': '15'},
    {'Fecha realizado': '11/11/24', 'Nombre': 'BULANY', 'Apellido 1': 'VADYM', 'Apellido 2': '', 'Dni': 'Z0428528C',
     'Móvil': '', 'Cómo nos ha conocido': 'Referido Anna U', 'Descripción': 'RECONSTRUCCIÓN', 'Doctor': 'ANNA',
     'Coste': '0,00', 'Importe': '80,00', 'Fecha última visita': '11/11/24', 'Fecha próxima cita': '',
     'Num. Doctor': '15'},
    {'Fecha realizado': '11/11/24', 'Nombre': 'LARISA', 'Apellido 1': 'KARIMOVA', 'Apellido 2': '', 'Dni': 'Y778163P',
     'Móvil': '651559048', 'Cómo nos ha conocido': 'Referido Anna U', 'Descripción': 'PRIMERA VISITA ODONTOLOGIA',
     'Doctor': 'ANNA', 'Coste': '0,00', 'Importe': '0,00', 'Fecha última visita': '11/11/24', 'Fecha próxima cita': '',
     'Num. Doctor': '15'},
    {'Fecha realizado': '11/11/24', 'Nombre': 'Vitalii', 'Apellido 1': 'Stepanenko', 'Apellido 2': '',
     'Dni': 'FY026540', 'Móvil': '', 'Cómo nos ha conocido': 'Referido Anna U', 'Descripción': 'RECONSTRUCCIÓN',
     'Doctor': 'ANNA', 'Coste': '0,00', 'Importe': '80,00', 'Fecha última visita': '11/11/24', 'Fecha próxima cita': '',
     'Num. Doctor': '15'},
    {'Fecha realizado': '11/11/24', 'Nombre': 'BULANY', 'Apellido 1': 'VADYM', 'Apellido 2': '', 'Dni': 'Z0428528C',
     'Móvil': '', 'Cómo nos ha conocido': 'Referido Anna U', 'Descripción': 'RECONSTRUCCIÓN', 'Doctor': 'ANNA',
     'Coste': '0,00', 'Importe': '80,00', 'Fecha última visita': '11/11/24', 'Fecha próxima cita': '',
     'Num. Doctor': '15'}
]

if __name__ == "__main__":
    DB_PATH = "output/data.db"
    # Define your date range (example: entire year 2023)
    start_date = datetime(2026, 1, 1)
    end_date = datetime(2026, 12, 31, 23, 59, 59)  # End of day for inclusivity

    print(f"--- Daily Check Data Retrieval ---")
    print(f"Date Range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    perform_appointment_checks(start_date, end_date)