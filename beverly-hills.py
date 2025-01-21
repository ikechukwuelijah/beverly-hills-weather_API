#%%
import requests
import pandas as pd
import json
import logging
from datetime import datetime
from sqlalchemy import create_engine

# Define the API URL
api_url = "https://api.openweathermap.org/data/2.5/weather?lat=34.0901&lon=-118.4065&appid=fc284336c861c52e8185c63082114ad5"

# Function to fetch and transform weather data into a DataFrame
def fetch_weather_data(url):
    try:
        # Send a GET request to the API
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors

        # Parse the JSON response
        weather_data = response.json()

        # Extract relevant data from the response
        data = {
            "Longitude": weather_data["coord"]["lon"],
            "Latitude": weather_data["coord"]["lat"],
            "Weather": weather_data["weather"][0]["main"],
            "Description": weather_data["weather"][0]["description"],
            "Temperature": weather_data["main"]["temp"],
            "Feels_Like": weather_data["main"]["feels_like"],
            "Temp_Min": weather_data["main"]["temp_min"],
            "Temp_Max": weather_data["main"]["temp_max"],
            "Pressure": weather_data["main"]["pressure"],
            "Humidity": weather_data["main"]["humidity"],
            "Visibility": weather_data["visibility"],
            "Wind_Speed": weather_data["wind"]["speed"],
            "Wind_Direction": weather_data["wind"]["deg"],
            "Cloudiness": weather_data["clouds"]["all"],
            "Sunrise": weather_data["sys"]["sunrise"],
            "Sunset": weather_data["sys"]["sunset"],
            "Timezone": weather_data["timezone"],
            "City_ID": weather_data["id"],
            "City_Name": weather_data["name"],
            "Code": weather_data["cod"]
        }

        # Convert the extracted data into a DataFrame
        df = pd.DataFrame([data])

        # Return the DataFrame
        return df

    except requests.exceptions.RequestException as e:
        print("An error occurred while accessing the API:", e)
        return None

# Database upload function
def upload_data(table, dataframe, upload_type):
    """
    Upload data to a specified table in the database.

    Parameters:
        table (str): Name of the table to upload data.
        dataframe (DataFrame): Pandas DataFrame containing data to upload.
        upload_type (str): Method of upload ('replace', 'append', etc.).

    Returns:
        None
    """
    try:
        logging.info("Attempting to connect to the database for uploading data.")
        # Create an SQLAlchemy engine for database connection
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={DATABASE_CONNECTION_STRING}")
        # Upload the DataFrame to the database table
        logging.info(f"Uploading data to table: {table}")
        dataframe.to_sql(table, engine, index=False, if_exists=upload_type, schema="dbo", chunksize=10000)
        logging.info(f"Data uploaded successfully to {table}.")
    except Exception as e:
        logging.error(f"Error uploading data: {e}")
        print(f"Error uploading data: {e}")


# Main execution
if __name__ == "__main__":
    # Set up logging to track script activity
    log_filename = f"log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    logging.basicConfig(filename=log_filename, level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

    # Add console logging for real-time feedback
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logging.getLogger().addHandler(console_handler)

    # Log the start of the script
    logging.info("Script started.")

    # Define the database connection string
    DATABASE_CONNECTION_STRING = (
        "Driver={ODBC Driver 17 for SQL Server};"
        "Server=ARMSTRONG;"
        "Database=weatherDB;"
        "Trusted_Connection=yes;"
    )

    # Fetch weather data and transform it into a DataFrame
    df = fetch_weather_data(api_url)

    if df is not None:
        # Specify the table name and upload type
        table_name = "beverly_hills"
        upload_type = "append"  # Options: 'replace', 'append'

        # Upload the DataFrame to the database
        try:
            upload_data(table_name, df, upload_type)
            logging.info("Data uploaded successfully.")
            print("Data uploaded successfully.")
        except Exception as e:
            logging.error(f"Failed to upload data: {e}")
            print(f"Failed to upload data: {e}")

    else:
        logging.error("Failed to fetch data, no data to upload.")

    # Log the end of the script
    logging.info("Script ended.")


# %%
