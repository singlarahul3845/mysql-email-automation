import pandas as pd
import mysql.connector
import logging
import requests
from io import BytesIO
import schedule
import time
from datetime import datetime, timedelta

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Global variable to store the last run time
last_run_time = None

# List of email extensions to exclude
excluded_email_extensions = ["gmail.com", "hotmail.com", "yahoo.com", "facebook.com", "OUTLOOK.COM", "googlemail.com", "icloud.com", "yahoo.fr", "web.de", "aol.com", "live.com", "hotmail.fr", "yahoo.co.uk", "hotmail.co.uk", "yahoo.co.in", "msn.com", "yahoo.com.br", "me.com", "mail.ru", "yopmail.com", "qq.com", "ymail.com", "rediffmail.com", "yahoo.de", "hotmail.it", "outlook.de", "hotmail.de", "yahoo.it", "yahoo.in", "yahoo.com.sg", "mail.com", "yahoo.com.mx", "yahoo.ca", "hotmail.es", "yandex.ru", "YAHOO.COM.AU", "yahoo.com.hk", "yahoo.com.tw", "gamil.com", "gmai.com", "yahoo.co.id", "yahoo.com.ph", "yahoo.gr", "freemail.hu", "freenet.de", "email.com"]  # Add any email extensions you want to exclude

# Function to connect to the MySQL database
def connect_to_db():
    try:
        logging.info("Attempting to connect to the database...")
        conn = mysql.connector.connect(
            host="localhost", 
            port=3306, 
            user="slideteam", 
            password="slideteam", 
            database="customer_data"
        )
        logging.info("Database connection established successfully.")
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to the database: {str(e)}")
        raise

# Function to check if the email exists in the database
def email_exists_in_db(cursor, email):
    try:
        query = "SELECT COUNT(*) FROM user_data_excluded WHERE Email = %s"
        cursor.execute(query, (email,))
        count = cursor.fetchone()[0]
        return count > 0
    except Exception as e:
        logging.error(f"Error checking email existence in database: {str(e)}")
        return False

# Function to fetch and read both Excel (.xlsx) and CSV (.csv) files from the URL
def fetch_file_from_url(url):
    try:
        logging.info(f"Fetching the file from URL: {url}")
        # Make a request to the URL to get the file
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
        response.raise_for_status()  # Raise an exception for bad responses (4xx or 5xx)

        # Determine the file extension (for deciding whether it's CSV or Excel)
        file_extension = url.split('.')[-1].lower()

        # If the file is Excel (.xlsx)
        if file_extension == 'xlsx':
            # Read the Excel content from the response
            excel_data = BytesIO(response.content)
            df = pd.read_excel(excel_data)
            logging.info(f"Excel file fetched successfully. Number of rows: {len(df)}")

        # If the file is CSV (.csv)
        elif file_extension == 'csv':
            # Read the CSV content from the response
            csv_data = BytesIO(response.content)
            df = pd.read_csv(csv_data)
            logging.info(f"CSV file fetched successfully. Number of rows: {len(df)}")

        else:
            logging.error("Unsupported file type. Only .xlsx and .csv are supported.")
            return None

        # Clean up the column names (same logic as before)
        logging.info(f"Columns before cleanup: {df.columns.tolist()}")
        df.columns = df.columns.str.strip().str.replace(',', '').str.replace(' ', '_')
        logging.info(f"Columns after cleanup: {df.columns.tolist()}")

        return df

    except Exception as e:
        logging.error(f"Error fetching or reading the file: {str(e)}")
        return None

# Function to insert the data from the file (Excel or CSV) into the database
def insert_data_from_file(df):
    if df is None:
        logging.error("No data to insert into the database.")
        return

    logging.info(f"Starting to insert {len(df)} records into the database...")

    # Connect to the database
    conn = connect_to_db()
    cursor = conn.cursor()

    # Loop through the rows in the DataFrame and insert the data
    for index, row in df.iterrows():
        logging.info(f"Processing row {index + 1}...")

        # Use corrected column names
        try:
            name = row['Name']
            email = row['Email']
            account_timestamp = row['Account_Timestamp']  # updated column name
            no_of_downloads = row['No_Of_Downloads']      # updated column name
            download_urls = row['Download_URLs'] if pd.notna(row['Download_URLs']) else None  # Replace NaN with None
            visited_urls = row['Visited_URLs'] if pd.notna(row['Visited_URLs']) else None  # Replace NaN with None
            free_or_paid = row['Free_or_Paid']            # updated column name
        except KeyError as e:
            logging.error(f"Missing column: {e}")
            continue

        # Check if the email ends with any of the excluded email extensions
        if any(email.endswith(ext) for ext in excluded_email_extensions):
            logging.info(f"Email {email} ends with an excluded extension. Skipping insert.")
            continue

        # Check if email already exists in the database
        if email_exists_in_db(cursor, email):
            logging.info(f"Email {email} already exists in the database. Skipping insert.")
            continue

        logging.info(f"Data for email {email}: Name = {name}, Account Timestamp = {account_timestamp}, No. of Downloads = {no_of_downloads}, Free or Paid = {free_or_paid}")

        # SQL query to insert the data into the database
        insert_query = """
        INSERT INTO 
user_data_excluded (Name, Email, Account_Timestamp, No_Of_Downloads, Download_URLs, Visited_URLs, Free_or_Paid)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        # Log the query being executed (for debugging purposes)
        logging.info(f"Executing query for {email}: {insert_query}")
        logging.info(f"Parameters: {name}, {email}, {account_timestamp}, {no_of_downloads}, {download_urls}, {visited_urls}, {free_or_paid}")

        try:
            cursor.execute(insert_query, (name, email, account_timestamp, no_of_downloads, download_urls, visited_urls, free_or_paid))
            logging.info(f"Inserted record for email: {email}")
        except Exception as e:
            logging.error(f"Failed to insert record for email: {email} - {str(e)}")

    # Commit the changes to the database
    try:
        conn.commit()
        logging.info("Successfully committed the changes to the database.")
    except Exception as e:
        logging.error(f"Failed to commit changes to the database: {str(e)}")

    # Close the connection
    cursor.close()
    conn.close()

    logging.info("Database insertion completed successfully!")

# Function to fetch and insert data at scheduled intervals
def schedule_task():
    global last_run_time

    excel_url = "https://slideteam:Zj7Ri%232XL%24w3w%40hJO@www.slideteam.net/pub/customer_data/customer_data.csv"  # Replace with your URL
    
    # Fetch and log the content from the file (either Excel or CSV)
    df = fetch_file_from_url(excel_url)

    # Insert data into MySQL if the file was fetched successfully
    if df is not None:
        insert_data_from_file(df)

    # Log the last run time and next scheduled time
    last_run_time = datetime.now()
    logging.info(f"Last run time: {last_run_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    next_run_time = last_run_time + timedelta(minutes=30)
    logging.info(f"Next scheduled run time: {next_run_time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Schedule the task to run again in 2 minutes
    schedule.every(30).minutes.do(schedule_task)

# Start the scheduled task
logging.info("Starting scheduled task. The first task will run immediately.")
schedule_task()

# Keep the script running to allow scheduling to work
while True:
    schedule.run_pending()
    
