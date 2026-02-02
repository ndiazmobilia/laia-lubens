import streamlit as st
import pandas as pd
import numpy as np
import time
from datetime import datetime, date, time as dt_time
import plotly.express as px

# --- 1. PAGE CONFIGURATION ---
st.set_page_config(
    page_title="Streamlit Component Gallery",
    page_icon="🧪",
    layout="wide",
    initial_sidebar_state="expanded",
)


# --- 2. FAKE DATA GENERATION ---
@st.cache_data
def get_fake_data():
    # Revenue over time - Use absolute values or uniform distribution for positive numbers
    chart_data = pd.DataFrame(
        np.abs(np.random.randn(20, 3) * 100),  # np.abs() ensures no negative values
        columns=['Odontología', 'Estética', 'Ortodoncia']
    )

    # Map data (Centered around Valencia, Spain)
    map_data = pd.DataFrame(
        np.random.randn(100, 2) / [50, 50] + [39.46, -0.37],
        columns=['lat', 'lon']
    )

    # Treatment Table
    df = pd.DataFrame({
        "Tratamiento": ["Dermapen", "Invisalign", "Limpieza", "Botox", "Implante"],
        "Precio": [150, 4500, 60, 350, 1200],
        "Estado": [True, True, False, True, False],
        "Fecha": [date(2023, 1, 1)] * 5
    })
    return chart_data, map_data, df

chart_data, map_data, df = get_fake_data()

# --- 3. SIDEBAR (Input Components) ---
with st.sidebar:
    st.title("Settings & Inputs")
    st.header("Selection Widgets")

    name = st.text_input("Clinic Name", "Lubens Clinic")
    role = st.selectbox("Your Role", ["Admin", "Doctor", "Receptionist"])
    branch = st.radio("Branch", ["Main City", "North Plaza", "South Coast"])
    access = st.multiselect("Dashboard Access", ["Financials", "Medical", "HR"], ["Financials"])

    st.divider()
    st.header("Sliders & Numbers")
    age_range = st.slider("Target Patient Age", 0, 100, (25, 55))
    capacity = st.select_slider("Clinic Capacity", options=["Low", "Medium", "High", "Full"])
    num_rooms = st.number_input("Operating Rooms", min_value=1, max_value=20, value=5)

    st.divider()
    st.header("Date & Time")
    appt_date = st.date_input("Schedule Date", date.today())
    appt_time = st.time_input("Opening Time", dt_time(9, 0))

    st.divider()
    st.header("Pickers & Files")
    color = st.color_picker("Brand Color", "#00f900")
    file = st.file_uploader("Upload Lab Report (PDF/SQL)")
    # Note: st.camera_input() exists but requires a camera to work

# --- 4. MAIN CONTENT (Layout & Display) ---
st.title(f"🚀 {name} Management Dashboard")
st.caption("This dashboard demonstrates every core Streamlit component using synthetic data.")

# --- TABS ---
tab1, tab2, tab3, tab4 = st.tabs(["📊 Analytics", "📋 Data Entry", "🖼️ Media", "🛠️ Status"])

with tab1:
    # --- METRICS ---
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Revenue", "€42,500", "12%")
    col2.metric("New Patients", "84", "-5%")
    col3.metric("Acceptance Rate", "68%", "2%")
    col4.metric("No-Shows", "4", "-1", delta_color="inverse")

    st.divider()

    # --- CHARTS ---
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Monthly Revenue Trend")
        st.line_chart(chart_data)
        st.subheader("Regional Patient Spread")
        st.map(map_data)
    with c2:
        st.subheader("Treatment Distribution")
        st.bar_chart(chart_data)
        st.subheader("Advanced Plotly Chart")
        fig = px.scatter(chart_data, x='Odontología', y='Estética', size='Ortodoncia', color='Ortodoncia')
        st.plotly_chart(fig, use_container_width=True)

with tab2:
    st.subheader("Database View")
    # Dataframe (Interactive)
    st.dataframe(df, use_container_width=True)

    # Table (Static)
    st.subheader("Static Table")
    st.table(df.head(3))

    # Form
    st.subheader("Treatment Form")
    with st.form("my_form"):
        st.write("Inside the form")
        f_name = st.text_input("Patient Name")
        f_treatment = st.selectbox("Treatment", ["Cleaning", "Extraction", "Whitening"])
        submitted = st.form_submit_button("Submit Record")
        if submitted:
            st.success(f"Record for {f_name} saved!")

    # JSON & Code
    st.subheader("System Config (JSON)")
    st.json({"version": "1.2.0", "status": "active", "db": "SQLite"})

    st.subheader("Snippet Example")
    st.code("print('Hello Clinic Performance!')", language='python')

with tab3:
    st.subheader("Media Components")
    m_col1, m_col2 = st.columns(2)
    with m_col1:
        st.image("https://images.unsplash.com/photo-1588776814546-1ffcf47267a5?w=500", caption="Clinic Interior")
    with m_col2:
        # Markdown & LaTeX
        st.markdown("**Bold Info**: Aesthetic treatments are up this quarter.")
        st.latex(r'''Revenue = \sum_{i=1}^{n} (Price_i \times Accepted_i)''')

    st.divider()
    st.video("https://www.youtube.com/watch?v=R2nr1uZ8dqc")  # Streamlit Intro Video

with tab4:
    st.subheader("Status & Notifications")

    # Progress & Spinner
    if st.button("Simulate Data Sync"):
        progress_bar = st.progress(0)
        with st.spinner('Syncing with SQL Database...'):
            for i in range(100):
                time.sleep(0.01)
                progress_bar.progress(i + 1)
        st.balloons()
        st.success("Sync Complete!")

    # Status boxes
    st.info("Reminder: Clinic is closed next Monday.")
    st.warning("Low stock: Dental Composite (A2 shade).")
    st.error("Connection Lost: Lab API not responding.")

    # Expander
    with st.expander("See technical logs"):
        st.write("2024-05-20 10:00:01 - User 'Beatriz' logged in.")
        st.write("2024-05-20 10:05:42 - Budget #1042 generated.")

    # Buttons
    st.link_button("Go to Clinic Website", "https://google.com")
    st.download_button("Download Monthly Report", data="Fake Report Content", file_name="report.txt")

# --- 5. FOOTER ---
st.divider()
if st.checkbox("Show raw data source"):
    st.write(chart_data)