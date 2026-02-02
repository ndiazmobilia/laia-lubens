
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


if date_option == "Today":
    start_date = today
    end_date = today
    prev_start_date = yesterday
    prev_end_date = yesterday
elif date_option == "Yesterday":
    start_date = yesterday
    end_date = yesterday
    prev_start_date = yesterday - timedelta(days=1)
    prev_end_date = yesterday - timedelta(days=1)
elif date_option == "Tomorrow":
    start_date = tomorrow
    end_date = tomorrow
    # No previous period for tomorrow
elif date_option == "This week":
    start_date = start_of_week
    end_date = end_of_week
    prev_start_date = start_of_last_week
    prev_end_date = end_of_last_week
elif date_option == "Last week":
    start_date = start_of_last_week
    end_date = end_of_last_week
    prev_start_date = start_of_last_week - timedelta(days=7)
    prev_end_date = end_of_last_week - timedelta(days=7)
elif date_option == "This month":
    start_date = start_of_month
    end_date = end_of_month
    prev_start_date = start_of_last_month
    prev_end_date = end_of_last_month
elif date_option == "Last month":
    start_date = start_of_last_month
    end_date = end_of_last_month
    prev_start_date = (start_of_last_month - timedelta(days=1)).replace(day=1)
    prev_end_date = start_of_last_month - timedelta(days=1)
elif date_option == "This year":
    start_date = start_of_year
    end_date = end_of_year
    prev_start_date = start_of_last_year
    prev_end_date = end_of_last_year
elif date_option == "Last year":
    start_date = start_of_last_year
    end_date = end_of_last_year
    prev_start_date = start_of_last_year.replace(year=start_of_last_year.year -1)
    prev_end_date = end_of_last_year.replace(year=end_of_last_year.year -1)
elif date_option == "Custom":
    start_date = st.sidebar.date_input("Start date", today)
    end_date = st.sidebar.date_input("End date", today)
    if start_date and end_date:
        delta_days = (end_date - start_date).days
        prev_start_date = start_date - timedelta(days=delta_days + 1)
        prev_end_date = end_date - timedelta(days=delta_days + 1)


if start_date and end_date:
    st.write(f"Selected date range: **{start_date.strftime('%Y-%m-%d')}** to **{end_date.strftime('%Y-%m-%d')}**")

    # --- Metrics ---
    col1, col2 = st.columns(2)

    # --- Revenue Metric ---
    current_revenue = get_revenue(start_date, end_date)
    previous_revenue = get_revenue(prev_start_date, prev_end_date) if prev_start_date and prev_end_date else 0

    delta_revenue = current_revenue - previous_revenue
    col1.metric("Revenue", f"{current_revenue:,.2f} €", f"{delta_revenue:,.2f} €")
