import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
import yaml
import numpy as np
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os



# Load City Coordinates
with open("src/config.yaml") as f:
    config = yaml.safe_load(f)

#Data Extraction 
cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

df_list = []
for city_name, coords in config["cities"].items():
	try:
		url = "https://api.open-meteo.com/v1/forecast"
		params = {
			"latitude": coords["latitude"],
			"longitude": coords["longitude"],
			"daily": [
				"temperature_2m_max",
				"temperature_2m_min",
				"rain_sum",
				"wind_speed_10m_max"
			],
			"timezone": "auto"
		}
		responses = openmeteo.weather_api(url, params=params)
		
		daily = responses[0].Daily()

		new_rows = {"date": pd.date_range(
			start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
			end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
			freq = pd.Timedelta(seconds = daily.Interval()),
			inclusive = "left"
		)}

		new_rows["city"] = city_name
		new_rows["max_temp_c"] = daily.Variables(0).ValuesAsNumpy()
		new_rows["min_temp_c"] = daily.Variables(1).ValuesAsNumpy()
		new_rows["temp_range_c"] = daily.Variables(0).ValuesAsNumpy() - daily.Variables(1).ValuesAsNumpy()
		new_rows["rain_sum"] = daily.Variables(2).ValuesAsNumpy()
		new_rows["max_wind_speed_kmh"] = daily.Variables(3).ValuesAsNumpy()
		
		city_dataframe = pd.DataFrame(data = new_rows)
		df_list.append(city_dataframe)

	except Exception as e:
		print(f"Failed to process {city_name}: {str(e)}")

weather_df = pd.concat(df_list, ignore_index=True)




# Data Transformation
weather_df["date"] = weather_df["date"].dt.date

conditions_rain = [
    (weather_df["rain_sum"] == 0),
    (weather_df["rain_sum"] > 0) & (weather_df["rain_sum"] <= 2),
    (weather_df["rain_sum"] > 2) & (weather_df["rain_sum"] <= 10),
    (weather_df["rain_sum"] > 10) & (weather_df["rain_sum"] <= 50),
    (weather_df["rain_sum"] > 50)
]

labels_rain = ["No Rain", "Light Rain", "Moderate Rain", "Heavy Rain", "Torrential Rain"]

conditions_wind = [
    (weather_df["max_wind_speed_kmh"] >= 0) & (weather_df["max_wind_speed_kmh"] < 1) ,
    (weather_df["max_wind_speed_kmh"] >= 1) & (weather_df["max_wind_speed_kmh"] < 6),
    (weather_df["max_wind_speed_kmh"] >= 6) & (weather_df["max_wind_speed_kmh"] < 12),
    (weather_df["max_wind_speed_kmh"] >= 12) & (weather_df["max_wind_speed_kmh"] < 20),
    (weather_df["max_wind_speed_kmh"] >= 20) & (weather_df["max_wind_speed_kmh"] < 29),
    (weather_df["max_wind_speed_kmh"] >= 29) & (weather_df["max_wind_speed_kmh"] < 39),
    (weather_df["max_wind_speed_kmh"] >= 39) & (weather_df["max_wind_speed_kmh"] < 50),
    (weather_df["max_wind_speed_kmh"] >= 50) & (weather_df["max_wind_speed_kmh"] < 62),
    (weather_df["max_wind_speed_kmh"] >= 62) & (weather_df["max_wind_speed_kmh"] < 75),
    (weather_df["max_wind_speed_kmh"] >= 75) & (weather_df["max_wind_speed_kmh"] < 89),
    (weather_df["max_wind_speed_kmh"] >= 89) & (weather_df["max_wind_speed_kmh"] < 103),
    (weather_df["max_wind_speed_kmh"] >= 103) & (weather_df["max_wind_speed_kmh"] < 118),
    (weather_df["max_wind_speed_kmh"] >= 118)
]

labels_wind = ["Calm", "Light Air", "Light Breeze", "Gentle Breeze", "Moderate Breeze", "Fresh Breeze", "Strong Breeze", "Moderate Gale", "Gale", "Strong Gale", "Storm", "Violent Storm", "Hurricane"]


weather_df["rain_status"] = np.select(conditions_rain, labels_rain, default="Unknown")
weather_df["wind_status"] = np.select(conditions_wind, labels_wind, default="Unknown")




# Loading

#PostgreSQL Credentials
load_dotenv()

DB_USER = os.getenv('DB_USER')
DB_PASSWORD =os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

data = weather_df.to_dict(orient='records')

with engine.connect() as conn:
    for row in data:
        query = text("""
            INSERT INTO weather (
                date, city, max_temp_c, min_temp_c, temp_range_c, 
                rain_sum, max_wind_speed_kmh, rain_status, wind_status
            )
            VALUES (
                :date, :city, :max_temp_c, :min_temp_c, :temp_range_c,
                :rain_sum, :max_wind_speed_kmh, :rain_status, :wind_status
            )
            ON CONFLICT (date, city) 
            DO UPDATE SET
                max_temp_c = EXCLUDED.max_temp_c,
                min_temp_c = EXCLUDED.min_temp_c,
                temp_range_c = EXCLUDED.temp_range_c,
                rain_sum = EXCLUDED.rain_sum,
                max_wind_speed_kmh = EXCLUDED.max_wind_speed_kmh,
                rain_status = EXCLUDED.rain_status,
                wind_status = EXCLUDED.wind_status
        """)
        conn.execute(query, row)
    conn.commit()

print("Finished.")