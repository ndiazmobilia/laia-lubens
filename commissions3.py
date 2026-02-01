from collections import defaultdict
from datetime import datetime
# CREATE TABLE comisiones ("Fecha" DATETIME, "Código" INTEGER PRIMARY KEY, "Paciente" TEXT, "Tratamiento" TEXT, "Diente" TEXT, "Descripción" TEXT, "Realizado" INTEGER, "Cobrado" INTEGER, "Seguro" INTEGER, "Costelab" REAL, "Costefinan" REAL, "Comisión" INTEGER, "Com" TEXT);
# Mapping Spanish month names to numerical values
spanish_months = {
    "Enero": 1,
    "Febrero": 2,
    "Marzo": 3,
    "Abril": 4,
    "Mayo": 5,
    "Junio": 6,
    "Julio": 7,
    "Agosto": 8,
    "Septiembre": 9,
    "Octubre": 10,
    "Noviembre": 11,
    "Diciembre": 12
}


def parse_number(value):
    try:
        return float(value.replace(",", ".")) if value else 0.0
    except ValueError:
        return 0.0


def format_number(value):
    return f"{value:.2f}".replace(".", ",")


def calculate_vat_exclusive(value):
    return value / 1.21



def perform_calculation(the_payments, all_doctors_commissions, doctor_id, month_name):

    def get_commission_type(doctor_id, description, como_nos_conocio):
        if doctor_id == "10" and "invisalign" in description.lower():
            return "invisalign"
        if doctor_id == "15" and como_nos_conocio == "Referido Anna U":
            return "referido"
        if doctor_id == "14" and como_nos_conocio == "Referido Juan":
            return "referido"
        return "regular"

    # Filter commissions for the given doctor
    # doctor_commissions = {c["commission_type"]: c["commission"] for c in doctors_commissions if c["id"] == doctor_id}
    doctor_commissions = [c for c in all_doctors_commissions if c["id"] == str(doctor_id)]

    # Initialize result structures
    commission_results = []
    alerts = []

    # Group payments by unique treatments using key
    # merged_payments = merge_entries(the_payments, month_name)
    # merged_payments = merge_entries_by_month_and_criteria(the_payments, month_name)
    merged_payments = merge_entries_in_pairs(the_payments, month_name)

    for payment in merged_payments:
        if parse_number(payment["Importe bruto"]) == 0:
            continue

        costo = payment["Costelab"]
        net_value = parse_number(payment["Importe bruto"])
        description = payment["Descripción"]
        commission_type = get_commission_type(doctor_id, description, payment["Código"])

        # Find the appropriate commission percentage
        doctor_commission = next(
            (c for c in doctor_commissions if c["commission_type"] == commission_type),
            None,
        )

        commission_percentage = doctor_commission["commission"]

        # Handle VAT for aesthetic medicine
        treatment_type = doctor_commission["treatment_type"]

        cobrado_sin_iva = calculate_vat_exclusive(net_value) if treatment_type == "aesthetic_medicine" else net_value
        costo_sin_iva = calculate_vat_exclusive(costo) if treatment_type == "aesthetic_medicine" else costo

        commission_amount = (cobrado_sin_iva - costo_sin_iva) * (commission_percentage / 100)

        commission_results.append({
            "Fecha": payment["Fecha"],
            "Paciente": payment["Paciente"],
            "Descripción": description,
            "Cobrado": net_value,
            "Cobrado sin IVA": format_number(cobrado_sin_iva),
            "Costo": format_number(parse_number(payment["Costelab"])),
            "Costo sin IVA": format_number(costo_sin_iva),
            "Tipo comisión": commission_type,
            "Porcentaje": format_number(commission_percentage),
            "Comisión": format_number(commission_amount),
        })
    return commission_results

# Main function to merge entries in pairs
def merge_entries_in_pairs(all_payments, month_name):
    merged_entries = []
    grouped = defaultdict(list)

    # Group entries by "Fecha", "Código", "Tratamiento", "Diente", "Descripción"
    for payment in all_payments:
        if is_matching_month(payment["Fecha"], month_name):
            key = (payment["Fecha"], payment["Código"], payment["Tratamiento"], payment["Diente"], payment["Descripción"])
            grouped[key].append(payment)

    # Process each group and merge in pairs
    for key, group in grouped.items():
        while len(group) >= 2:
            entry1 = group.pop(0)
            entry2 = group.pop(0)

            # Merge the pair
            merged_entry = {
                "Fecha": entry1["Fecha"],
                "Código": entry1["Código"],
                "Paciente": entry1["Paciente"],
                "Tratamiento": entry1["Tratamiento"],
                "Diente": entry1["Diente"],
                "Descripción": entry1["Descripción"],
                "Realizado": "{:.2f}".format(max(parse_number(entry1["Realizado"]), parse_number(entry2["Realizado"]))),
                "Cobrado": "{:.2f}".format(max(parse_number(entry1["Cobrado"]), parse_number(entry2["Cobrado"]))),
                "Seguro": entry1["Seguro"],  # Take from first entry (adjust if needed)
                "Coste lab.": entry1["Coste lab."],  # Take from first entry (adjust if needed)
                "Coste finan.": entry1["Coste finan."],  # Take from first entry (adjust if needed)
                "Comisión%": entry1["Comisión%"],  # Take from first entry (adjust if needed)
                "Com.": entry1["Com."],  # Take from first entry (adjust if needed)
                "Importe bruto": "{:.2f}".format(max(
                    parse_number(entry1["Realizado"]), parse_number(entry1["Cobrado"]),
                    parse_number(entry2["Realizado"]), parse_number(entry2["Cobrado"])
                )),
            }
            merged_entries.append(merged_entry)

        # If there's an unmatched entry left, add it as is
        # CREATE TABLE comisiones ("Fecha" DATETIME, "Código" INTEGER PRIMARY KEY, "Paciente" TEXT, "Tratamiento" TEXT, "Diente" TEXT, "Descripción" TEXT, "Realizado" INTEGER, "Cobrado" INTEGER, "Seguro" INTEGER, "Costelab" REAL, "Costefinan" REAL, "Comisión" INTEGER, "Com" TEXT);
        if group:
            entry = group.pop(0)
            entry["Importe bruto"] = "{:.2f}".format(max(entry["Realizado"], entry["Cobrado"]))
            merged_entries.append(entry)

    return merged_entries


def merge_entries_by_month_and_criteria(entries, month_in_spanish):
    def parse_number(value):
        """Convert a string with comma as decimal separator to float."""
        return float(value.replace(",", "."))

    def spanish_month_to_number(spanish_month):
        """Convert Spanish month to corresponding month number."""
        months = {
            "Enero": 1, "Febrero": 2, "Marzo": 3, "Abril": 4, "Mayo": 5,
            "Junio": 6, "Julio": 7, "Agosto": 8, "Septiembre": 9,
            "Octubre": 10, "Noviembre": 11, "Diciembre": 12
        }
        return months.get(spanish_month.capitalize(), None)

    # Get month number from the Spanish month name
    target_month = spanish_month_to_number(month_in_spanish)
    if not target_month:
        raise ValueError("Invalid Spanish month provided.")

    # Filter entries by the target month
    filtered_entries = [
        entry for entry in entries if datetime.strptime(entry["Fecha"], "%d/%m/%y").month == target_month
    ]

    merged_results = []
    processed_indices = set()

    for i, entry1 in enumerate(filtered_entries):
        if i in processed_indices:
            continue

        max_realizado = parse_number(entry1["Realizado"])
        max_cobrado = parse_number(entry1["Cobrado"])
        importe_bruto = max(max_realizado, max_cobrado)

        for j, entry2 in enumerate(filtered_entries):
            if i != j and j not in processed_indices:
                if (
                    entry1["Fecha"] == entry2["Fecha"] and
                    entry1["Código"] == entry2["Código"] and
                    entry1["Tratamiento"] == entry2["Tratamiento"] and
                    entry1["Diente"] == entry2["Diente"] and
                    entry1["Descripción"] == entry2["Descripción"]
                ):
                    # Merge the entries
                    max_realizado = max(max_realizado, parse_number(entry2["Realizado"]))
                    max_cobrado = max(max_cobrado, parse_number(entry2["Cobrado"]))
                    processed_indices.add(j)

        # Create the merged entry
        merged_results.append({
            **entry1,  # Base the result on the first entry
            "Realizado": f"{max_realizado:.2f}".replace(".", ","),
            "Cobrado": f"{max_cobrado:.2f}".replace(".", ","),
            "Importe bruto": f"{importe_bruto:.2f}".replace(".", ",")
        })
        processed_indices.add(i)

    return merged_results



# Helper function to check if the entry's month matches the Spanish month
def is_matching_month(fecha, month_name):
    # Convert the "Fecha" (dd/mm/yy) to a datetime object
    # datefmt='%Y-%m-%d %H:%M:%S'
    fecha_obj = datetime.strptime(fecha, "%Y-%m-%dT%H:%M:%S")
    # Convert the month number to Spanish month name
    spanish_months_2 = {
        1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril", 5: "Mayo", 6: "Junio",
        7: "Julio", 8: "Agosto", 9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
    }
    return spanish_months_2.get(fecha_obj.month) == month_name


# Main function to merge entries
def merge_entries(entries, month_name):
    merged_entries = []
    # Use a dictionary to group entries by the matching fields
    grouped = defaultdict(list)

    # Group entries by "Fecha", "Código", "Tratamiento", "Diente", "Descripción"
    for payment in entries:
        if is_matching_month(payment["Fecha"], month_name):
            key = (
            payment["Fecha"], payment["Código"], payment["Tratamiento"], payment["Diente"], payment["Descripción"])
            grouped[key].append(payment)

    # Process each group and merge
    for key, group in grouped.items():
        if len(group) == 1:
            # If there is only one entry, keep it as is
            merged_entries.append(group[0])
        else:
            # For multiple entries, merge them by taking the max value of "Realizado" and "Cobrado"
            merged_entry = group[0]
            for payment in group[1:]:
                # Merge based on max "Realizado" and "Cobrado"
                merged_entry["Realizado"] = max(merged_entry["Realizado"], payment["Realizado"])
                merged_entry["Cobrado"] = max(merged_entry["Cobrado"], payment["Cobrado"])

                # Add "Importe bruto" field, which is the max of Realizado or Cobrado
            merged_entry["Importe bruto"] = max(parse_number(merged_entry["Realizado"]),
                                                parse_number(merged_entry["Cobrado"]))

            if parse_number(merged_entry["Realizado"]) != 0 and parse_number(merged_entry["Cobrado"]) != 0:
                # Append merged entry
                merged_entries.append(merged_entry)

    return merged_entries

# Example Usage:
payments = [
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
doctors_commissions = [
    {"id": "10", "name": "Macarena Remohi Martínez-Medina", "treatment_type": "dentistry", "commission_type": "regular",
     "commission": 60},
    {"id": "10", "name": "Macarena Remohi Martínez-Medina", "treatment_type": "dentistry",
     "commission_type": "invisalign", "commission": 50},
]
patients = {
    "440": "seguros dentales",
    "657": "Referido Anna U",
    "661": "Referido Juan"
}
