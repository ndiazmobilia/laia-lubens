import logging
import traceback
from datetime import datetime, timedelta
from daily_checks import get_data_for_date_range
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s - %(asctime)s - %(module)s - %(funcName)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def perform_appointment_reminders():
    try:
        # Get today's date
        today = datetime.now()
        tomorrow = datetime.now() + timedelta(days=1)
        print(today)
        print(tomorrow)
        # Get appointments for tomorrow
        daily_appointments = get_data_for_date_range(
            db_path="output/data.db",
            table_name="citas",
            date_column="Fecha",
            start_date=today,
            end_date=tomorrow
        )
        logging.info(f'all appointments {daily_appointments}')
        the_result = {
            "success": "true",
            "data": extract_appointments_to_remind(daily_appointments)}
        pass
    except Exception:
        the_result = {
            "success": "false",
            "error": {
                "code": "DB_ERROR",
                "message": "There was an error getting daily appointments from db"}
        }
        logging.error(traceback.print_exc())
    return the_result





def extract_appointments_to_remind(appointments):
    """
    Matches unconfirmed appointments with patient information.

    Parameters:
        appointments (list of dict): List of appointments.

    Returns:
        list of dict: A list of dictionaries containing the appointment patient name,
                      and suggestions of matching full names with phone numbers.
    """
    unconfirmed = []

    # Clean and process appointments
    for appointment in appointments:
        if appointment.get('confirmacion de la visita', '').strip() not in ['Confirmada', 'Cancelada']:
            appt_name = appointment.get('paciente', '').strip().lower()  # Normalize case to lowercase
            telephone = appointment.get('telefono', '').strip().lower().replace(" ", "")
            if appt_name:
                unconfirmed.append({
                    'appointment_name': appt_name.title(),  # Restore title casing for output
                    'telephone': telephone
                })
    return unconfirmed


if __name__ == "__main__":
    DB_PATH = "output/data.db"
    # Define your date range (example: entire year 2023)

    print(f"--- Appointments to confirm ---")
    print(perform_appointment_reminders())