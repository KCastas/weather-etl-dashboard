import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import datetime


# PostgreSQL Credentials
load_dotenv() 

DB_USER = os.getenv('DB_USER')
DB_PASSWORD =os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')


# Fetching Available Data
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

cities = pd.read_sql("SELECT DISTINCT city FROM weather;", engine)["city"].tolist()
selected_city = st.selectbox("Select a city:", cities)

query = f"""
    SELECT * FROM weather 
    WHERE city = '{selected_city}'
    ORDER BY date DESC 
    LIMIT 100
"""
df = pd.read_sql(query, engine)

# Fetching Today's Data For Selected City
today = datetime.datetime.now().strftime("%Y-%m-%d")

query = f"""
    SELECT * FROM weather 
    WHERE city = '{selected_city}' 
    AND date = '{today}'
    LIMIT 1
"""
df_today = pd.read_sql(query, engine)

# Dashboard UI
st.title(f"🌤️ Today's Weather Forecast for {selected_city}")
if not df_today.empty:
    latest_data = df_today.iloc[0]
else:
    st.warning("No data available for today. Showing latest available data instead.")
    latest_data = df.iloc[0]


col1, col2, col3 = st.columns(3)
col1.metric("Max Temp (°C)", f"{latest_data['max_temp_c']:.1f}")
col2.metric("Min Temp (°C)", f"{latest_data['min_temp_c']:.1f}")
col3.metric("Rain Sum (mm)", f"{latest_data['rain_sum']:.1f}")

st.subheader("🚨 Weather Alerts")


# Wind Alert
wind_status = latest_data["wind_status"]
wind_colors = {
    "Calm": "✅", 
    "Light Air": "✅",
    "Moderate Breeze": "⚠️",
    "Strong Breeze": "⚠️",
    "Gale": "❌",
    "Hurricane": "🔥 EXTREME WARNING"
}
st.write(f"{wind_colors.get(wind_status, '')} **Wind:** {wind_status} ({round(latest_data['max_wind_speed_kmh'], 2)} km/h)")

# Rain Alert
rain_status = latest_data["rain_status"]
rain_colors = {
    "No Rain": "✅",
    "Light Rain": "🌧️",
    "Heavy Rain": "⚠️",
    "Torrential Rain": "❌ FLOOD RISK"
}
st.write(f"{rain_colors.get(rain_status, '')} **Rain:** {rain_status} ({round(latest_data['rain_sum'], 2)} mm)")




# Temperature & Rain/Wind Graphs
st.header("Weather Trends")

tab1, tab2 = st.tabs(["Temperature", "Rain & Wind"])
with tab1:
    fig_temp = px.line(df, x="date", y=["max_temp_c", "min_temp_c"], 
                      title=f"Temperature in {selected_city} (°C)")
    st.plotly_chart(fig_temp)

with tab2:
    fig_rain = px.bar(df, x="date", y="rain_sum", 
                     title=f"Rainfall in {selected_city} (mm)")
    fig_wind = px.line(df, x="date", y="max_wind_speed_kmh", 
                      title=f"Wind Speed in {selected_city} (km/h)")
    st.plotly_chart(fig_rain)
    st.plotly_chart(fig_wind)