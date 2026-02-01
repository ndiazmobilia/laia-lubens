import calendar
import logging
import sqlite3
import traceback
from datetime import datetime

from commissions3 import perform_calculation

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s - %(asctime)s - %(module)s - %(funcName)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# la limitacion de este "meto do" de saber el treatment type de acuerdo al doctor es que no podra haber un
# doctor que realice distintos typos tratamientos (por ejemplo que Anna Ucrania haga dentiste y estetica)
doctors_commissions = [
    {"id": "15", "name": "Anna Pevrukhina", "treatment_type": "dentistry", "commission_type": "regular", "commission": 35},
    {"id": "15", "name": "Anna Pevrukhina", "treatment_type": "dentistry", "commission_type": "referido", "commission": 50},
    {"id": "14", "name": "Juan Millet", "treatment_type": "dentistry", "commission_type": "regular", "commission": 35},
    {"id": "14", "name": "Juan Millet", "treatment_type": "dentistry", "commission_type": "referido", "commission": 50},
    {"id": "10", "name": "Macarena Remohi Martínez-Medina", "treatment_type": "dentistry", "commission_type": "regular", "commission": 60},
    {"id": "10", "name": "Macarena Remohi Martínez-Medina", "treatment_type": "dentistry", "commission_type": "invisalign", "commission": 50},
    {"id": "11", "name": "Alejandro Cordero", "treatment_type": "dentistry", "commission_type": "regular", "commission": 35},
    {"id": "2", "name": "Agusti Ferrando i Estrella", "treatment_type": "dentistry", "commission_type": "regular", "commission": 35},
    {"id": "20", "name": "Melissa Rivera", "treatment_type": "aesthetic_medicine", "commission_type": "regular", "commission": 50},
    {"id": "23", "name": "Carmen Herrero", "treatment_type": "aesthetic_medicine", "commission_type": "regular", "commission": 60},
    {"id": "22", "name": "Dayana Arizaka Riquelme", "treatment_type": "aesthetic_medicine", "commission_type": "regular", "commission": 50},
]


def perform_calculate_commissions(month, doctor_id):
    results = []
    try:
        results = perform_commission_calculation(doctors_commissions, doctor_id, month)
        results = {"success": "true", "data": results}
    except Exception:
        logging.error("There was an error scraping patients in Cliniwin")
        results = {
            "success": "false",
            "error": {
                "code": "SCRAPING_ERROR",
                "message": "There was an error scraping patients in Cliniwin"}
        }
        logging.error(traceback.print_exc())
    return results


def get_payments_with_patient_info(db_path: str, start_date: datetime, end_date: datetime) -> list[dict]:
    """
    Retrieves payments from the cobros table within a given date range,
    joining with the datos_personales table to include patient 'como nos conocio' information.

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
        # CREATE TABLE comisiones ("Fecha" DATETIME, "Código" INTEGER PRIMARY KEY, "Paciente" TEXT, "Tratamiento" TEXT, "Diente" TEXT, "Descripción" TEXT, "Realizado" INTEGER, "Cobrado" INTEGER, "Seguro" INTEGER, "Costelab" REAL, "Costefinan" REAL, "Comisión" INTEGER, "Com" TEXT);
        # Construct the query with a LEFT JOIN
        query = """
            SELECT c.*, dp.Cómonoshaconocido
            FROM comisiones c
            LEFT JOIN datos_personales dp ON c.Código = dp.Código
            WHERE c.Fecha BETWEEN ? AND ?
        """

        cursor.execute(query, (start_date_str, end_date_str))

        rows = cursor.fetchall()

        # Convert sqlite3.Row objects to dictionaries
        data = [dict(row) for row in rows]
        return data

    except sqlite3.Error as e:
        print(f"SQLite error in get_payments_with_patient_info: {e}")
        return []
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return []
    finally:
        if conn:
            conn.close()


def perform_commission_calculation(all_doctors_commissions, doctor_id, month_name):
    # Map Spanish month names to numbers
    spanish_months = {
        "Enero": 1, "Febrero": 2, "Marzo": 3, "Abril": 4, "Mayo": 5, "Junio": 6,
        "Julio": 7, "Agosto": 8, "Septiembre": 9, "Octubre": 10, "Noviembre": 11, "Diciembre": 12
    }
    
    # Get the current year and the month number
    current_year = datetime.now().year
    month_number = spanish_months.get(month_name.capitalize())
    
    if not month_number:
        raise ValueError("Invalid Spanish month provided.")

    # Get the first and last day of the month
    _, last_day = calendar.monthrange(current_year, month_number)
    start_date = datetime(current_year, month_number, 1)
    end_date = datetime(current_year, month_number, last_day, 23, 59, 59)

    # Get payments from the database
    the_payments = get_payments_with_patient_info("output/data.db", start_date, end_date)
    print(the_payments)
    return perform_calculation(the_payments, all_doctors_commissions, doctor_id, month_name)


def perform_calculation2(payments, all_doctors_commissions, doctor_id, patients):
    vat = 21  # VAT percentage
    commission_results = []
    total_commission = 0
    total_for_clinic = 0

    # Get commission percentages for the specified doctor
    doctor_commissions = [c for c in all_doctors_commissions if c["id"] == str(doctor_id)]

    for payment in payments:
        # Ignore payments not attached to any doctor
        if not payment["Código"]:
            continue

        # Determine treatment type and commission type
        patient_code = payment["Código"]
        description = payment.get("Descripción", "").lower()
        commission_type = "regular"  # Default

        if doctor_id == "10" and "invisalign" in description:
            commission_type = "invisalign"
        elif doctor_id == "15" and patients.get(patient_code) == "Referido Anna U":
            commission_type = "referido"
        elif doctor_id == "14" and patients.get(patient_code) == "Referido Juan":
            commission_type = "referido"

        # Find the appropriate commission percentage
        doctor_commission = next(
            (c for c in doctor_commissions if c["commission_type"] == commission_type),
            None,
        )

        if not doctor_commission:
            continue

        commission_percentage = doctor_commission["commission"]

        # Handle VAT for aesthetic medicine
        treatment_type = doctor_commission["treatment_type"]
        amount_cobrado = float(payment["Cobrado"].replace(",", "."))
        costo_lab = float(payment["Coste lab."].replace(",", "."))

        if treatment_type == "aesthetic_medicine":
            cobrado_sin_iva = round(amount_cobrado / (1 + vat / 100), 2)
            costo_sin_iva = round(costo_lab / (1 + vat / 100), 2)
        else:
            cobrado_sin_iva = amount_cobrado
            costo_sin_iva = costo_lab

        # Calculate commission
        net_amount = cobrado_sin_iva - costo_sin_iva
        commission_amount = round((net_amount * commission_percentage) / 100, 2)
        # Update totals
        total_commission += commission_amount
        total_for_clinic += (net_amount - commission_amount)

        # Prepare detailed result
        commission_results.append({
            "Fecha": payment["Fecha"],
            "Paciente": payment["Paciente"],
            "Descripción": payment["Descripción"],
            "Cobrado": payment["Cobrado"],
            "Cobrado sin IVA": f"{cobrado_sin_iva:.2f}",
            "Costo": payment["Coste lab."],
            "Costo sin IVA": f"{costo_sin_iva:.2f}",
            "Tipo comisión": commission_type,
            "Porcentaje": f"{commission_percentage:.2f}",
            "Comisión": f"{commission_amount:.2f}",
        })

    # Use a set to store unique codes
    unique_patients_amount = len({payment["Código"] for payment in payments})
    # Ensure no division by zero
    productivity = total_for_clinic / unique_patients_amount if unique_patients_amount > 0 else 0
    doctor_analytics = {
        "Total a pagar": f"{total_commission:.2f}",
        "Total para clinica": f"{total_for_clinic:.2f}",
        "Cantidad pacientes": unique_patients_amount,
        "Productividad por paciente": f"{productivity:.2f}"
    }

    return [commission_results, doctor_analytics]


# Example usage
payments_example = [
    {
        "Fecha": "04/09/24",
        "Código": "440",
        "Paciente": "MAYRA  DE BIASE  MONTESERIN",
        "Tratamiento": "",
        "Diente": "0",
        "Descripción": "FERULA PROTECCION DE INJERTO",
        "Realizado": "0,00",
        "Cobrado": "70,00",
        "Seguro": "0,00",
        "Coste lab.": "0,00",
        "Coste finan.": "0,00",
        "Comisión%": "0,00",
        "Com.": ""
    }
]

patients_example = {
    "440": "seguros dentales",
    "657": "Referido Anna U",
    "661": "Referido Juan",
    "662": "Referido Anna U",
    "663": "seguros dentales",
    "664": "Facebook",
    "669": "",
    "671": "INSTAGRAM",
    "675": "Amigos"
}

# results, total = perform_calculation(payments, doctors_commissions, "15", patients)

# results = perform_calculate_commissions("Noviembre", "23")

# results = perform_commission_calculation(f'downloads/comisiones_nov_dayana.xls',
#                                                  f'{download_path}/cliniwin_patients_example.xls',
#                                                  doctors_commissions,
#                                                  "22",
#                                          "Noviembre")
# print(json.dumps(results, indent=2, ensure_ascii=False))


# Run the function and print the result
# parsed_data = parse_patients(f'{download_path}/cliniwin_patients_example.xls')
# print(json.dumps(parsed_data, indent=4, ensure_ascii=False))

# parsed_data = parse_payments(f'{download_path}/cliniwin_payments_example.xls')
# #print(parsed_data)
# print(json.dumps(parsed_data[:3], indent=2, ensure_ascii=False))
# print(json.dumps(parsed_data, indent=4, ensure_ascii=False))

print(perform_calculate_commissions("Enero", "15"))
# perform_calculate_commissions("2024-11-01", "2024-11-30")

# Run the function and print the results
# summary, details = perform_commissions_calculation(payments, doctors_commissions)


# print("Summary of Commissions by Doctor:")
# for row in summary:
#     print(row)

# print("\nDetailed Payments:")
# for payment in details:
#     print(payment)
