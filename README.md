# Project Overview

This project consists of multiple Python scripts designed to interact with a MySQL database and automate tasks like fetching data from URLs, processing and inserting that data into a database, sending personalized emails, and handling scheduled operations.

## Files

### 1. `mysql_connection_all_data.py`
This script is designed to fetch data from a URL (either in CSV or Excel format), clean the data, and insert it into a MySQL database. It includes the following functionalities:
- **Database Connection**: Establishes a connection to the `customer_data` MySQL database using `mysql.connector`.
- **Data Fetching**: Retrieves files from URLs (CSV or Excel) and processes them into a pandas DataFrame.
- **Data Insertion**: Loops through the rows of the DataFrame and inserts valid records into the `all_user_data` table.
- **Scheduling**: The script runs at regular intervals (every 30 minutes) to fetch new data and insert it into the database.

### 2. `mysql_connection_for_excluded_data.py`
Similar to the first script, this script processes data from URLs, but it focuses on excluding certain email domains. The key differences include:
- **Excluded Email Extensions**: A predefined list of email domains (such as Gmail, Yahoo, etc.) that are excluded from being inserted into the database.
- **Database Insertions**: Data is inserted into the `user_data_excluded` table instead of `all_user_data`.
- **Scheduled Execution**: The script runs at regular intervals, ensuring new data is inserted periodically, excluding specific email addresses.

### 3. `new_program_for_email.py`
This script is designed to send personalized emails to users based on their browsing history. The process involves:
- **Email Content Generation**: The script generates email content dynamically by using the user's browsing history and OpenAI's API to create a tailored email.
- **HTML Conversion**: Converts plain-text email content into HTML format, ensuring proper hyperlinking and layout.
- **MySQL Integration**: Retrieves users from the `user_data_excluded` table who have not yet received emails and updates their status once the email is sent.
- **SMTP Integration**: Sends personalized emails using SMTP with Gmail's server, with the option to include CC addresses and log the progress of email dispatch.

## Requirements

To run the scripts, ensure you have the following libraries installed:
- `pandas`
- `mysql-connector-python`
- `requests`
- `schedule`
- `beautifulsoup4`
- `smtplib`
- `logging`
- `json`

### Installation

1. Install the required dependencies:
   ```bash
   pip install pandas mysql-connector-python requests schedule beautifulsoup4
