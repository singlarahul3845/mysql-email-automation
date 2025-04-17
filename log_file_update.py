import requests
import logging
from bs4 import BeautifulSoup
import time
import schedule
# Set up logging to output time directly to the console
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(message)s')

# Properly encoded URL with authentication (percent-encoded special characters)
url = "https://slideteam:Zj7Ri%232XL%24w3w%40hJO@www.slideteam.net/pub/customer_data/"

# Function to send request, scrape the time, and log it
def send_request_and_log():

        
        # Send GET request to the URL
        response = requests.get(url)
        
        # Log the status code
        
        # Check if the response is successful
        if response.status_code == 200:
            
            # Parse the HTML content
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find the <pre> tag and extract its content
            pre_tag = soup.find('pre')
            if pre_tag:
                # Extract the relevant part from the <pre> tag (in your case, timestamp is part of it)
                pre_content = pre_tag.get_text(strip=True)
                
                # Extract the timestamp part (assuming the time is in the format '17-Apr-2025 09:30')
                timestamp = pre_content.split()[-2] + " " + pre_content.split()[-1]
                
                # Log the timestamp to the console
                logging.info(f"Timestamp found: {timestamp}")
          
        


# Run the function once to test
schedule.every(30).minutes.do(send_request_and_log)
while True:
    schedule.run_pending()
    time.sleep(1)
