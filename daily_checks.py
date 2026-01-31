import difflib
import logging
import os
import shutil
import traceback
from collections import defaultdict
from datetime import datetime
from typing import List, Dict, Any

from dotenv import load_dotenv

from browser import download_path
from browser import get_chrome_instance
from cliniwin import download_cliniwin_treatment_stats
from cliniwin_parser import parse_treatments
from doctoralia import download_doctoralia_appointments
from doctoralia_parser import parse_appointments

# Load environment variables from .env file
load_dotenv()

user = os.getenv('CLINIWIN_USERNAME')
password = os.getenv('CLINIWIN_PASSWORD')

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
    "7": "María Florencia Cerviche"
}


def perform_check_appointments(from_date, to_date):
    result = []
    chrome = get_chrome_instance()
    appointments_file_name = None
    treatments_stats_file_name = None
    try:
        appointments_file_name = download_doctoralia_appointments(chrome, from_date, to_date)
        try:
            treatments_stats_file_name = download_cliniwin_treatment_stats(chrome, from_date, to_date, user, password)
            result = perform_appointment_checks(f'{download_path}/{appointments_file_name}',
                                                f'{download_path}/{treatments_stats_file_name}')
            result = {"success": "true", "data": result}
        except Exception:
            result = {
                "success": "false",
                "error": {
                    "code": "SCRAPING_ERROR",
                    "message": "There was an error scraping treatment stats in Cliniwin"}
            }
            logging.error("There was an error scraping Cliniwin")
            logging.error(traceback.print_exc())
            screenshot_filename = f'/{download_path}/screenshot-cliniwin.png'
            chrome.save_screenshot(screenshot_filename)
            pass
        pass
    except Exception:
        result = {
            "success": "false",
            "error": {
                "code": "SCRAPING_ERROR",
                "message": "There was an error scraping appointments in Doctoralia"}
        }
        logging.error("There was an error scraping Doctoralia")
        logging.error(traceback.print_exc())
        screenshot_filename = f'/{download_path}/screenshot-doctoralia.png'
        chrome.save_screenshot(screenshot_filename)
    finally:
        # Code that will run no matter what
        logging.info("Quitting ChromeDriver")
        chrome.quit()
        # Path to the WebDriverManager cache directory
        logging.info("Removing WebDriverManager cache directory")
        cache_dir = os.path.expanduser("~/.wdm")
        # Delete the cache directory if it exists
        if os.path.exists(cache_dir):
            shutil.rmtree(cache_dir)
        if os.path.exists(appointments_file_name):
            logging.info(f"Removing file {appointments_file_name}.")
            os.remove(appointments_file_name)
        if os.path.exists(treatments_stats_file_name):
            logging.info(f"Removing file {treatments_stats_file_name}.")
            os.remove(treatments_stats_file_name)

    return result


def perform_appointment_checks(appointments_file_name, treatments_file_name):
    appointments = parse_appointments(appointments_file_name)
    treatments = parse_treatments(treatments_file_name)

    appointments = [
        appointment for appointment in appointments
        if appointment['Paciente'] not in (None, '')
    ]

    # Normalize dates in doctoralia_appointments
    for appt in appointments:
        appt['Fecha_normalizada'] = normalize_date(appt['Fecha'], '%d/%m/%Y')

    # Normalize dates in cliniwin_treatments
    for treatment in treatments:
        treatment['Fecha_normalizada'] = normalize_date(treatment['Fecha realizado'], '%d/%m/%y')

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
    return alerts


def check_state(appointments):
    allowed_statuses = {'Visita realizada', 'No ha venido', 'Cancelada'}
    alerts = []

    # Check `Estado` in doctoralia_appointments
    for appointment in appointments:
        if appointment['Estado'] not in allowed_statuses:
            alerts.append({
                "type": "Estado invalido",
                "message": f"El paciente {appointment['Paciente']} con fecha {appointment['Fecha']} no tiene estado en Visita realizada, No ha venido, o Cancelada",
                "data": appointment
            })

    return alerts


def normalize_name(name: str) -> str:
    """Normalize a name by converting to lowercase and removing extra spaces."""
    return " ".join(name.lower().strip().split())


def generate_suggestions(target_name: str, candidates: List[str]) -> List[str]:
    """Generate a list of suggested names using difflib for close matches."""
    return difflib.get_close_matches(target_name, candidates, n=3, cutoff=0.5)


def check_treatments(appointments: List[Dict[str, Any]], treatments: List[Dict[str, Any]]) -> list[
    dict[str, str | dict[str, Any]]]:
    alerts = []

    # Build a list of normalized names from cliniwin_treatments
    cliniwin_names = [
        normalize_name(f"{entry['Nombre']} {entry['Apellido 1']} {entry['Apellido 2']}".strip())
        for entry in treatments
    ]

    for appointment in appointments:
        # Process only appointments with 'Estado' == 'Visita realizada'
        if appointment['Estado'] != 'Visita realizada':
            continue

        patient_name = normalize_name(appointment['Paciente'])

        # Check if the patient is in cliniwin_treatments
        if patient_name not in cliniwin_names:
            suggestions = generate_suggestions(patient_name, cliniwin_names)
            alerts.append({
                "type": "Tratamiento inexistente",
                "message": f"El paciente {appointment['Paciente']} con fecha {appointment['Fecha']} no tiene un tratamiento realizado en Cliniwin. Sugerencias: {suggestions or 'Ninguna'}",
                "data": appointment
            })

    return alerts


def match_patient_name(patient_name: str, cliniwin_names: List[str]) -> str:
    """Find the best match for a patient's name using difflib."""
    matches = difflib.get_close_matches(patient_name, cliniwin_names, n=1, cutoff=0.5)
    return matches[0] if matches else None


def check_doctors(
        appointments: List[Dict[str, str]],
        treatments: List[Dict[str, str]]
) -> list[dict[str, str | dict[str, str]]]:
    alerts = []
    logging.info(f'this is the treatments: {treatments}')
    logging.info(f'this is the appointments: {appointments}')

    # Create a mapping of normalized names to cliniwin treatments
    cliniwin_mapping = {
        normalize_name(f"{entry['Nombre']} {entry['Apellido 1']} {entry['Apellido 2']}"):
            entry for entry in treatments
    }

    logging.info(f'this is the cliniwin mapping: {cliniwin_mapping}')
    for appointment in appointments:
        if appointment['Estado'] != 'Visita realizada':
            continue

        # Normalize the patient name from Doctoralia
        patient_name = normalize_name(appointment['Paciente'])

        # Find the best match for the patient in Cliniwin
        matched_name = match_patient_name(patient_name, list(cliniwin_mapping.keys()))

        if not matched_name:
            continue

        cliniwin_entry = cliniwin_mapping[matched_name]

        # Compare doctor numbers
        doctoralia_doctor_name = appointment['Especialista']
        doctoralia_doctor_number = [key for key, value in doctors.items() if value == doctoralia_doctor_name]
        doctor_en_cliniwin = cliniwin_entry['Num. Doctor']
        # que no sea el doctor 18 que es claudia
        if doctoralia_doctor_number and doctoralia_doctor_number[
            0] != doctor_en_cliniwin and doctor_en_cliniwin != '18':
            cliniwin_doctor_name = doctors.get(doctor_en_cliniwin, "Desconocido")
            alerts.append({
                "type": "Doctor erroneo",
                "message": f"Doctor no coincide para el paciente {appointment['Paciente']}: Doctoralia ({doctoralia_doctor_name}) vs. Cliniwin ({cliniwin_doctor_name}).",
                "data": appointment
            })

    return alerts


# Step 1: Normalize the dates to ISO 8601 format
def normalize_date(date_str, input_format, output_format='%Y-%m-%d'):
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

doctoralia_appointments_file_name = "/home/ndiaz/workspace/appointment-daily-checks/downloads/doctoralia_appointments_from_date_to_date.1731883112200.xlsx"
cliniwin_treatments_file_name = "/home/ndiaz/workspace/appointment-daily-checks/downloads/cliniwin_treatments_stats_from_date_to_date.1731883142005.xls"

# perform_appointment_checks(doctoralia_appointments_file_name, cliniwin_treatments_file_name)
