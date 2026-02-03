
import streamlit as st
from datetime import date, timedelta
import sqlite3
import pandas as pd

st.title("Clínica Lubens Dashboard")

# --- Database Connection ---
DB_PATH = "output/data.db"

def get_revenue(start_date, end_date):
    """Queries the database to get the total revenue for a given date range."""
    try:
        query = f"""
                SELECT SUM(Importecobrado)
                FROM cobros
                WHERE Fechadecobro BETWEEN '{start_date}' AND '{end_date}'
            """
        with sqlite3.connect(DB_PATH) as con:
            total_revenue = pd.read_sql_query(query, con).iloc[0, 0]
            return total_revenue if total_revenue is not None else 0
    except (sqlite3.Error, FileNotFoundError) as e:
        st.error(f"Database error: {e}")
        return 0
    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")
        return 0

def get_new_patients(start_date, end_date):
    """Queries the database to get the number of new patients for a given date range."""
    try:
        query = f"""
                SELECT COUNT(*)
                FROM fechas_pacientes
                WHERE Fechadealta BETWEEN '{start_date}' AND '{end_date}'
            """
        with sqlite3.connect(DB_PATH) as con:
            new_patients = pd.read_sql_query(query, con).iloc[0, 0]
            return new_patients if new_patients is not None else 0
    except (sqlite3.Error, FileNotFoundError) as e:
        st.error(f"Database error: {e}")
        return 0
    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")
        return 0


def get_new_appointments(start_date, end_date):
    """Queries the database to get the number of new appointments for a given date range."""
    try:
        query = f"""
                SELECT COUNT(*)
                FROM citas
                WHERE Fecha BETWEEN '{start_date}' AND '{end_date}'
            """
        with sqlite3.connect(DB_PATH) as con:
            new_appointments = pd.read_sql_query(query, con).iloc[0, 0]
            return new_appointments if new_appointments is not None else 0
    except (sqlite3.Error, FileNotFoundError) as e:
        st.error(f"Database error: {e}")
        return 0
    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")
        return 0

def get_treatment_distribution(start_date, end_date):
    """Queries the database to get the distribution of treatments for a given date range."""
    try:
        query = f"""
                SELECT
                    Especialidad,
                    COUNT(*) as treatment_count
                FROM tratamientos
                WHERE Fecharealizado BETWEEN '{start_date}' AND '{end_date}'
                GROUP BY Especialidad
                ORDER BY treatment_count DESC
            """
        with sqlite3.connect(DB_PATH) as con:
            df = pd.read_sql_query(query, con)
            return df
    except (sqlite3.Error, FileNotFoundError) as e:
        st.error(f"Database error for treatment distribution: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"An unexpected error occurred for treatment distribution: {e}")
        return pd.DataFrame()

def get_total_unique_patients(start_date, end_date):
    """Queries the database to get the total number of unique patients who received treatments for a given date range."""
    try:
        query = f"""
                SELECT COUNT(DISTINCT CódigoPaciente)
                FROM tratamientos
                WHERE Fecharealizado BETWEEN '{start_date}' AND '{end_date}'
            """
        with sqlite3.connect(DB_PATH) as con:
            total_patients = pd.read_sql_query(query, con).iloc[0, 0]
            return total_patients if total_patients is not None else 0
    except (sqlite3.Error, FileNotFoundError) as e:
        st.error(f"Database error for total unique patients: {e}")
        return 0
    except Exception as e:
        st.error(f"An unexpected error occurred for total unique patients: {e}")
        return 0

def get_aesthetic_total_spending(start_date, end_date):
    """Queries the database to get the total spending for 'Estetica' treatments for a given date range."""
    try:
        query = f"""
                SELECT SUM(Precio)
                FROM tratamientos
                WHERE Especialidad = 'ESTETICA' AND Fecharealizado BETWEEN '{start_date}' AND '{end_date}'
            """
        with sqlite3.connect(DB_PATH) as con:
            total_spending = pd.read_sql_query(query, con).iloc[0, 0]
            return total_spending if total_spending is not None else 0
    except (sqlite3.Error, FileNotFoundError) as e:
        st.error(f"Database error for aesthetic total spending: {e}")
        return 0
    except Exception as e:
        st.error(f"An unexpected error occurred for aesthetic total spending: {e}")
        return 0

def get_unique_aesthetic_patients(start_date, end_date):
    """Queries the database to get the number of unique patients who received 'Estetica' treatments for a given date range."""
    try:
        query = f"""
                SELECT COUNT(DISTINCT CódigoPaciente)
                FROM tratamientos
                WHERE Especialidad = 'Estetica' AND Fecharealizado BETWEEN '{start_date}' AND '{end_date}'
            """
        with sqlite3.connect(DB_PATH) as con:
            unique_patients = pd.read_sql_query(query, con).iloc[0, 0]
            return unique_patients if unique_patients is not None else 0
    except (sqlite3.Error, FileNotFoundError) as e:
        st.error(f"Database error for unique aesthetic patients: {e}")
        return 0
    except Exception as e:
        st.error(f"An unexpected error occurred for unique aesthetic patients: {e}")
        return 0

def get_average_spending_per_patient(start_date, end_date):
    """Calculates the average spending per patient for a given date range."""
    total_revenue = get_revenue(start_date, end_date)
    total_unique_patients = get_total_unique_patients(start_date, end_date)
    if total_unique_patients > 0:
        return total_revenue / total_unique_patients
    return 0

def get_average_spending_per_aesthetic_patient(start_date, end_date):
    """Calculates the average spending per aesthetic medicine patient for a given date range."""
    aesthetic_total_spending = get_aesthetic_total_spending(start_date, end_date)
    unique_aesthetic_patients = get_unique_aesthetic_patients(start_date, end_date)
    if unique_aesthetic_patients > 0:
        return aesthetic_total_spending / unique_aesthetic_patients
    return 0


# New helper function for granularity
def get_granularity(start_date, end_date):
    delta_days = (end_date - start_date).days
    if delta_days <= 60:
        return "day"
    elif 60 < delta_days <= 730:
        return "month"
    else:
        return "year"

def get_revenue_by_period(start_date, end_date, granularity):
    try:
        if granularity == "day":
            period_format = '%Y-%m-%d'
        elif granularity == "month":
            period_format = '%Y-%m'
        else: # year
            period_format = '%Y'

        query = f"""
                SELECT
                    strftime('{period_format}', Fechadecobro) as period,
                    SUM(Importecobrado) as total_revenue
                FROM cobros
                WHERE Fechadecobro BETWEEN '{start_date}' AND '{end_date}'
                GROUP BY period
                ORDER BY period
            """
        with sqlite3.connect(DB_PATH) as con:
            df = pd.read_sql_query(query, con)
            return df
    except (sqlite3.Error, FileNotFoundError) as e:
        st.error(f"Database error for revenue by period: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"An unexpected error occurred for revenue by period: {e}")
        return pd.DataFrame()

def get_new_patients_by_period(start_date, end_date, granularity):
    try:
        if granularity == "day":
            period_format = '%Y-%m-%d'
        elif granularity == "month":
            period_format = '%Y-%m'
        else: # year
            period_format = '%Y'

        query = f"""
                SELECT
                    strftime('{period_format}', Fechadealta) as period,
                    COUNT(*) as new_patients_count
                FROM fechas_pacientes
                WHERE Fechadealta BETWEEN '{start_date}' AND '{end_date}'
                GROUP BY period
                ORDER BY period
            """
        with sqlite3.connect(DB_PATH) as con:
            df = pd.read_sql_query(query, con)
            return df
    except (sqlite3.Error, FileNotFoundError) as e:
        st.error(f"Database error for new patients by period: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"An unexpected error occurred for new patients by period: {e}")
        return pd.DataFrame()

def get_new_appointments_by_period(start_date, end_date, granularity):
    """Queries the database to get the number of new appointments for a given date range, grouped by period."""
    try:
        if granularity == "day":
            period_format = '%Y-%m-%d'
        elif granularity == "month":
            period_format = '%Y-%m'
        else: # year
            period_format = '%Y'

        query = f"""
                SELECT
                    strftime('{period_format}', Fecha) as period,
                    COUNT(*) as new_appointments_count
                FROM citas
                WHERE Fecha BETWEEN '{start_date}' AND '{end_date}'
                GROUP BY period
                ORDER BY period
            """
        with sqlite3.connect(DB_PATH) as con:
            df = pd.read_sql_query(query, con)
            return df
    except (sqlite3.Error, FileNotFoundError) as e:
        st.error(f"Database error for new appointments by period: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"An unexpected error occurred for new appointments by period: {e}")
        return pd.DataFrame()

def get_total_patients_by_period(start_date, end_date, granularity):
    """Queries the database to get the total number of unique patients who received treatments for a given date range, grouped by period."""
    try:
        if granularity == "day":
            period_format = '%Y-%m-%d'
        elif granularity == "month":
            period_format = '%Y-%m'
        else: # year
            period_format = '%Y'

        query = f"""
                SELECT
                    strftime('{period_format}', Fecharealizado) as period,
                    COUNT(DISTINCT CódigoPaciente) as total_patients_count
                FROM tratamientos
                WHERE Fecharealizado BETWEEN '{start_date}' AND '{end_date}'
                GROUP BY period
                ORDER BY period
            """
        with sqlite3.connect(DB_PATH) as con:
            df = pd.read_sql_query(query, con)
            return df
    except (sqlite3.Error, FileNotFoundError) as e:
        st.error(f"Database error for total patients by period: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"An unexpected error occurred for total patients by period: {e}")
        return pd.DataFrame()

def get_average_spending_per_patient_by_period(start_date, end_date, granularity):
    """Queries the database to get the average spending per patient for a given date range, grouped by period."""
    try:
        if granularity == "day":
            period_format = '%Y-%m-%d'
        elif granularity == "month":
            period_format = '%Y-%m'
        else: # year
            period_format = '%Y'

        query = f"""
                SELECT
                    period,
                    AVG(patient_spending) as avg_spending_per_patient
                FROM (
                    SELECT
                        CódigoPaciente,
                        strftime('{period_format}', Fecharealizado) as period,
                        SUM(Precio) as patient_spending
                    FROM tratamientos
                    WHERE Fecharealizado BETWEEN '{start_date}' AND '{end_date}'
                    GROUP BY CódigoPaciente, period
                )
                GROUP BY period
                ORDER BY period
            """
        with sqlite3.connect(DB_PATH) as con:
            df = pd.read_sql_query(query, con)
            return df
    except (sqlite3.Error, FileNotFoundError) as e:
        st.error(f"Database error for average spending per patient by period: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"An unexpected error occurred for average spending per patient by period: {e}")
        return pd.DataFrame()


# --- Sidebar for date selection ---
st.sidebar.title("Date Selection")

today = date.today()
yesterday = today - timedelta(days=1)
tomorrow = today + timedelta(days=1)

start_of_week = today - timedelta(days=today.weekday())
end_of_week = start_of_week + timedelta(days=6)
start_of_last_week = start_of_week - timedelta(days=7)
end_of_last_week = start_of_week - timedelta(days=1)

start_of_month = today.replace(day=1)
end_of_month = (start_of_month + timedelta(days=32)).replace(day=1) - timedelta(days=1)
start_of_last_month = (start_of_month - timedelta(days=1)).replace(day=1)
end_of_last_month = start_of_month - timedelta(days=1)


start_of_year = today.replace(month=1, day=1)
end_of_year = today.replace(month=12, day=31)
start_of_last_year = start_of_year.replace(year=today.year - 1)
end_of_last_year = end_of_year.replace(year=today.year - 1)


date_option = st.sidebar.selectbox(
    "Select a date range:",
    (
        "Today",
        "Yesterday",
        "Tomorrow",
        "This week",
        "Last week",
        "This month",
        "Last month",
        "This year",
        "Last year",
        "Custom",
    ),
)

start_date, end_date = None, None
prev_start_date, prev_end_date = None, None
comparison_label = ""


if date_option == "Today":
    start_date = today
    end_date = today
    prev_start_date = yesterday
    prev_end_date = yesterday
    comparison_label = "vs yesterday"
elif date_option == "Yesterday":
    start_date = yesterday
    end_date = yesterday
    prev_start_date = yesterday - timedelta(days=1)
    prev_end_date = yesterday - timedelta(days=1)
    comparison_label = "vs previous day"
elif date_option == "Tomorrow":
    start_date = tomorrow
    end_date = tomorrow
    # No previous period for tomorrow
    comparison_label = ""
elif date_option == "This week":
    start_date = start_of_week
    end_date = end_of_week
    prev_start_date = start_of_last_week
    prev_end_date = end_of_last_week
    comparison_label = "vs last week"
elif date_option == "Last week":
    start_date = start_of_last_week
    end_date = end_of_last_week
    prev_start_date = start_of_last_week - timedelta(days=7)
    prev_end_date = end_of_last_week - timedelta(days=7)
    comparison_label = "vs previous week"
elif date_option == "This month":
    start_date = start_of_month
    end_date = end_of_month
    prev_start_date = start_of_last_month
    prev_end_date = end_of_last_month
    comparison_label = "vs last month"
elif date_option == "Last month":
    start_date = start_of_last_month
    end_date = end_of_last_month
    prev_start_date = (start_of_last_month - timedelta(days=1)).replace(day=1)
    prev_end_date = start_of_last_month - timedelta(days=1)
    comparison_label = "vs previous month"
elif date_option == "This year":
    start_date = start_of_year
    end_date = end_of_year
    prev_start_date = start_of_last_year
    prev_end_date = end_of_last_year
    comparison_label = "vs last year"
elif date_option == "Last year":
    start_date = start_of_last_year
    end_date = end_of_last_year
    prev_start_date = start_of_last_year.replace(year=start_of_last_year.year - 1)
    prev_end_date = end_of_last_year.replace(year=end_of_last_year.year - 1)
    comparison_label = "vs previous year"
elif date_option == "Custom":
    start_date = st.sidebar.date_input("Start date", today)
    end_date = st.sidebar.date_input("End date", today)
    if start_date and end_date:
        delta_days = (end_date - start_date).days
        prev_start_date = start_date - timedelta(days=delta_days + 1)
        prev_end_date = end_date - timedelta(days=delta_days + 1)
        comparison_label = "vs previous period"


if start_date and end_date:
    st.write(
        f"Selected date range: **{start_date.strftime('%Y-%m-%d')}** to **{end_date.strftime('%Y-%m-%d')}**"
    )

    # --- Metrics ---
    col1, col2, col3, col4 = st.columns(4)

    # --- Revenue Metric ---
    current_revenue = get_revenue(start_date, end_date)
    previous_revenue = (
        get_revenue(prev_start_date, prev_end_date)
        if prev_start_date and prev_end_date
        else 0
    )

    delta_revenue = current_revenue - previous_revenue
    delta_revenue_str = f"{delta_revenue:,.2f} €"
    if comparison_label:
        delta_revenue_str += f" {comparison_label}"

    col1.metric("Revenue", f"{current_revenue:,.2f} €", delta_revenue_str)

    # --- New Patients Metric ---
    current_new_patients = get_new_patients(start_date, end_date)
    previous_new_patients = (
        get_new_patients(prev_start_date, prev_end_date)
        if prev_start_date and prev_end_date
        else 0
    )

    delta_new_patients = current_new_patients - previous_new_patients
    delta_new_patients_str = f"{delta_new_patients}"
    if comparison_label:
        delta_new_patients_str += f" {comparison_label}"
    col2.metric("New Patients", f"{current_new_patients}", delta_new_patients_str)

    # --- New Appointments Metric ---
    current_new_appointments = get_new_appointments(start_date, end_date)
    previous_new_appointments = (
        get_new_appointments(prev_start_date, prev_end_date)
        if prev_start_date and prev_end_date
        else 0
    )

    delta_new_appointments = current_new_appointments - previous_new_appointments
    delta_new_appointments_str = f"{delta_new_appointments}"
    if comparison_label:
        delta_new_appointments_str += f" {comparison_label}"
    col3.metric(
        "New Appointments", f"{current_new_appointments}", delta_new_appointments_str
    )

    # --- Average Spending per Patient Metric ---
    current_avg_spending = get_average_spending_per_patient(start_date, end_date)
    previous_avg_spending = (
        get_average_spending_per_patient(prev_start_date, prev_end_date)
        if prev_start_date and prev_end_date
        else 0
    )
    delta_avg_spending = current_avg_spending - previous_avg_spending
    delta_avg_spending_str = f"{delta_avg_spending:,.2f} €"
    if comparison_label:
        delta_avg_spending_str += f" {comparison_label}"
    col4.metric("Avg Spending/Patient", f"{current_avg_spending:,.2f} €", delta_avg_spending_str)

    # --- Average Spending per Aesthetic Patient Metric ---
    current_avg_aesthetic_spending = get_average_spending_per_aesthetic_patient(start_date, end_date)
    previous_avg_aesthetic_spending = (
        get_average_spending_per_aesthetic_patient(prev_start_date, prev_end_date)
        if prev_start_date and prev_end_date
        else 0
    )
    delta_avg_aesthetic_spending = current_avg_aesthetic_spending - previous_avg_aesthetic_spending
    delta_avg_aesthetic_spending_str = f"{delta_avg_aesthetic_spending:,.2f} €"
    if comparison_label:
        delta_avg_aesthetic_spending_str += f" {comparison_label}"

    # --- Charts ---
    st.subheader("Trends over time")

    granularity = get_granularity(start_date, end_date)

    # Revenue Chart
    revenue_df = get_revenue_by_period(start_date, end_date, granularity)
    if not revenue_df.empty:
        st.write(f"### Revenue by {granularity.capitalize()}")
        st.bar_chart(revenue_df, x='period', y='total_revenue')
    else:
        st.info("No revenue data available for the selected period.")

    # New Patients Chart
    new_patients_df = get_new_patients_by_period(start_date, end_date, granularity)
    total_patients_df = get_total_patients_by_period(start_date, end_date, granularity)

    if not new_patients_df.empty or not total_patients_df.empty:
        # Merge the two dataframes
        patients_df = pd.merge(new_patients_df, total_patients_df, on='period', how='outer').fillna(0)
        patients_df['patients_excluding_new'] = patients_df['total_patients_count'] - patients_df['new_patients_count']
        
        # Ensure the columns for charting are present, even if one is all zeros
        chart_data = patients_df[['period', 'new_patients_count', 'patients_excluding_new']]
        
        st.write(f"### Patients by {granularity.capitalize()}")
        st.bar_chart(chart_data, x='period', y=['new_patients_count', 'patients_excluding_new'])
    else:
        st.info("No patient data available for the selected period.")

    # New Appointments Chart
    appointments_df = get_new_appointments_by_period(start_date, end_date, granularity)
    if not appointments_df.empty:
        st.write(f"### New Appointments by {granularity.capitalize()}")
        st.bar_chart(appointments_df, x='period', y='new_appointments_count')
    else:
        st.info("No new appointments data available for the selected period.")

    # Average Spending per Patient Chart
    avg_spending_df = get_average_spending_per_patient_by_period(start_date, end_date, granularity)
    if not avg_spending_df.empty:
        st.write(f"### Average Spending per Patient by {granularity.capitalize()}")
        st.bar_chart(avg_spending_df, x='period', y='avg_spending_per_patient')
    else:
        st.info("No average spending per patient data available for the selected period.")

    # Treatment Distribution Chart
    treatment_distribution_df = get_treatment_distribution(start_date, end_date)
    if not treatment_distribution_df.empty:
        st.write("### Treatment Distribution")
        st.bar_chart(treatment_distribution_df, x='Especialidad', y='treatment_count')
    else:
        st.info("No treatment data available for the selected period.")
