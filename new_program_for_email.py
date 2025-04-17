import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import logging
import mysql.connector
import time
import schedule
import json
import requests
from datetime import datetime, timedelta
import re
from bs4 import BeautifulSoup

def convert_to_html(plain_text, add_get_best_offers=True):
    """Convert plain text email to HTML with proper hyperlinks"""
    # Replace newlines with HTML breaks
    html_content = plain_text.replace("\n", "<br>")
    
    # Add phone link
    html_content = html_content.replace(
        "+1-408-215-1583", 
        "<b><a href='tel:+14082151583'>+1-408-215-1583</a></b>"
    )
    
    # Remove any three dots lines which create extra space
    html_content = html_content.replace("<br>...<br>", "<br>")
    html_content = html_content.replace("<br>···<br>", "<br>")
    
    # Check if "Book A Free Consultation" already exists in the content
    if "Book A Free Consultation" in html_content and add_get_best_offers:
        # Replace with hyperlinked version
        html_content = html_content.replace(
            "Book A Free Consultation", 
            "<b><a href='https://www.slideteam.net/questionnaire/'>Book A Free Consultation</a></b>"
        )
    elif add_get_best_offers:
        # Look for the common closing phrases to insert the CTA before them
        closing_phrases = ["Best regards,", "Regards,", "Sincerely,", "Thanks,", "Thank you,", "Warm regards,", "Yours truly,"]
        
        for phrase in closing_phrases:
            if phrase in html_content:
                # Insert the CTA before the closing phrase with proper spacing
                closing_index = html_content.find(phrase)
                html_content = (
                    html_content[:closing_index] + 
                    "<b><a href='https://www.slideteam.net/questionnaire/'>Book A Free Consultation</a></b>" + 
                    html_content[closing_index:]
                )
                return html_content
        
        # If no closing phrase is found, add it at the end with proper spacing
        html_content += "<b><a href='https://www.slideteam.net/questionnaire/'>Book A Free Consultation</a></b>"
    
    return html_content

def process_urls_and_extract_titles(visited_urls_str, include_urls_in_output=False):
    """
    Process a string of visited URLs:
    1. Split into individual URLs
    2. Clean the URLs (remove trailing commas, quotes, whitespace)
    3. Filter out URLs in the exclusion list
    4. Scrape the title tag from each remaining URL
    5. Return a formatted string of page titles (with or without URLs)
    
    Args:
        visited_urls_str: String containing the URLs
        include_urls_in_output: Boolean, whether to include URLs in the output or just titles
    """
    # URLs to exclude
    excluded_urls = [
        "https://www.slideteam.net/pricing",
        "https://www.slideteam.net/about-us",
        "https://www.slideteam.net/contacts",
        "https://www.slideteam.net/pricing#faq",
        "https://www.slideteam.net/terms-of-use",
        "https://www.slideteam.net/privacy-policy",
        "https://www.slideteam.net/coupon-code"
    ]
    
    # Split the string of URLs based on common delimiters
    urls = []
    if visited_urls_str:
        if '\n' in visited_urls_str:
            # Split by newlines first
            raw_urls = visited_urls_str.split('\n')
        else:
            # Split by commas if no newlines
            raw_urls = visited_urls_str.split(',')
        
        # Clean each URL properly
        for raw_url in raw_urls:
            # Strip whitespace, quotes, and trailing commas
            clean_url = raw_url.strip().strip('"').strip("'").rstrip(',').strip()
            
            # Only add non-empty URLs
            if clean_url:
                urls.append(clean_url)
    
    # Log the cleaned URLs for debugging
    logging.info(f"Cleaned URLs for processing: {urls}")
    
    # Filter out excluded URLs
    filtered_urls = []
    for url in urls:
        is_excluded = False
        for excluded_url in excluded_urls:
            if url.startswith(excluded_url):
                is_excluded = True
                break
        
        if not is_excluded:
            filtered_urls.append(url)
    
    # If no URLs remain after filtering, return a message
    if not filtered_urls:
        return "No specific product pages were visited."
    
    # Extract titles from the remaining URLs
    titles_data = []
    
    for url in filtered_urls:
        try:
            # Add headers to mimic a browser request
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            }
            
            # Request the page
            logging.info(f"Attempting to fetch title from URL: {url}")
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()  # Raise an exception for 4XX/5XX responses
            
            # Parse HTML
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract title
            title_tag = soup.find('title')
            
            if title_tag and title_tag.string:
                title = title_tag.string.strip()
                if include_urls_in_output:
                    titles_data.append(f" Page Title:{title} (URL: {url})")
                else:
                    # Only include the title without the URL
                    titles_data.append(f"{title}")
            else:
                if include_urls_in_output:
                    titles_data.append(f"URL visited but no title found: {url}")
                else:
                    titles_data.append(f"Visited a page but no title found")
                
        except requests.exceptions.HTTPError as e:
            if '404' in str(e):
                logging.warning(f"Page not found (404) for URL: {url}")
                if include_urls_in_output:
                    titles_data.append(f"URL visited (page no longer exists): {url}")
                else:
                    titles_data.append(f"Visited a page that no longer exists")
            else:
                logging.warning(f"HTTP error scraping title from {url}: {str(e)}")
                if include_urls_in_output:
                    titles_data.append(f"URL visited: {url} (couldn't retrieve title)")
                else:
                    titles_data.append(f"Visited a page (couldn't retrieve title)")
        except Exception as e:
            logging.warning(f"Error scraping title from {url}: {str(e)}")
            if include_urls_in_output:
                titles_data.append(f"URL visited: {url} (couldn't retrieve title)")
            else:
                titles_data.append(f"Visited a page (couldn't retrieve title)")
    
    # Format the output
    return "\n".join(titles_data)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Dictionary for senders, their app passwords, and display names
senders = {
    "Shivam.bansal@slideteam.net": {
        "password": "vboh prot htnx mbol",
        "display_name": "Shivam Bansal"
    },
    "ron.jennifer@slideteam.net": {
        "password": "xpfg ygnp shmr rdqe",
        "display_name": "Ron Jennifer"
    },
    "sadie.sarah@slideteam.net": {
        "password": "mtjp qbgj kpab cdoe",
        "display_name": "Sadie Sarah"
    },
    "eloise.mark@slideteam.net": {
        "password": "pomd gxgo owaa fedp",
        "display_name": "Eloise Mark"
    },
    "natalia.john@slideteam.net": {
        "password": "sgqz kyhn uwta wrxd",
        "display_name": "Natalia John"
    }
}

# OpenAI API Key - Replace with your actual OpenAI API key
OPENAI_API_KEY = "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"

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

# Function to get users where emails haven't been sent yet
def get_unsent_user_data():
    try:
        conn = connect_to_db()
        cursor = conn.cursor(dictionary=True)  # Return results as dictionaries

        # Get all records where email_sent = 0 or NULL
        query = """
        SELECT id, email, name, visited_urls 
        FROM user_data_excluded 
        WHERE email_sent = 0 OR email_sent IS NULL
        ORDER BY id
        """
        cursor.execute(query)

        # Fetch all data
        user_data = cursor.fetchall()

        cursor.close()
        conn.close()

        # Log the number of users found
        logging.info(f"Found {len(user_data)} users who haven't been sent emails yet.")

        return user_data

    except Exception as e:
        logging.error(f"Error fetching unsent user data: {str(e)}")
        return []

# Function to update email_sent status
def update_email_sent_status(user_id, status):
    try:
        conn = connect_to_db()
        cursor = conn.cursor()

        # Update the email_sent field for the given user_id
        query = """
        UPDATE user_data_excluded 
        SET email_sent = %s 
        WHERE id = %s
        """
        cursor.execute(query, (status, user_id))
        conn.commit()

        cursor.close()
        conn.close()

        logging.info(f"Updated email_sent status to {status} for user_id {user_id}")
        return True

    except Exception as e:
        logging.error(f"Error updating email_sent status for user_id {user_id}: {str(e)}")
        return False

# Function to generate personalized email content using OpenAI
def generate_email_content(name, email, visited_urls, sender_name, sender_email):
    try:
        # Check if name is empty or None, replace with "Valued Customer" if so
        if not name or name.strip() == "":
            name = "Valued Customer"
            
        # Process URLs and extract title content instead of using raw URLs
        # Set include_urls_in_output to False to exclude URLs from the prompt
        page_titles = process_urls_and_extract_titles(visited_urls, include_urls_in_output=False)
            
        # Prepare the prompt with user data
        prompt = f"""
{{Name}} who's email address is {{Email}}, visited multiple pages on my website.
Here is information about the pages they visited: 
{{Page_Titles}}

Read this information and try to understand what {name} is looking for on my website slideteam. On the basis of their browsing history(visited pages), write an email where I can pitch {name} my presentation services which includes presentation design, content research, and everything related to presentation.

1. Don't include topic in email
2. Include generic information about the topics in email, and make it a click bait email with subject line
3. Don't tell {{Name}} that we are tracking their browsing history
4. Introduce yourself as a presentation designer
5. Give CTA to this number <a href="tel:+14082151583">+1-408-215-1583</a>
6. Add this line at the end of the email: "Book A Free Consultation"
7. Very important: After "Regards," or "Best regards," or similar closing, DO NOT include any name, contact information, or position/title. End the email with just the closing (e.g., "Best regards,") and nothing else after that.
IMPORTANT: Format your response with "Subject:" followed by the email subject on the first line, then the email body after that.
        """

        # Replace the placeholders with actual data
        prompt = prompt.replace("{Name}", name)
        prompt = prompt.replace("{Email}", email)
        prompt = prompt.replace("{Page_Titles}", page_titles)
        
        # Log the prompt being sent to OpenAI
        logging.info(f"Sending prompt to OpenAI for {email}:\n{prompt}")

        # Prepare the API request
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {OPENAI_API_KEY}"
        }
        
        data = {
            "model": "gpt-4o-mini",
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.5
        }

        # Make the API call
        response = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers=headers,
            data=json.dumps(data)
        )

        # Parse the response
        if response.status_code == 200:
            result = response.json()
            email_content = result["choices"][0]["message"]["content"]
            
            # Extract subject line and body
            lines = email_content.strip().split('\n')
            subject = ""
            body = email_content
            
            # Look for "Subject:" in the first few lines
            for i, line in enumerate(lines[:5]):  # Check first 5 lines for subject
                if line.lower().startswith("subject:"):
                    subject = line[8:].strip()  # Extract subject without the "Subject:" prefix
                    body = '\n'.join(lines[i+1:]).strip()  # The rest is the body
                    break
            
            # If no subject found, use a default one
            if not subject:
                logging.warning(f"No subject line found in OpenAI response for {email}. Using default subject.")
                subject = f"Custom Presentation Services for {name}"
            
            # Replace common name placeholders with the actual sender name
            replacement_patterns = [
                r'\[your name\]', 
                r'\[your full name\]', 
                r'\[name\]', 
                r'\[my name\]',
                r'\[NAME\]',
                r'\[Your Name\]',
                r'\[My Name\]',
                r'\[Full Name\]'
            ]
            
            # Replace all patterns case-insensitively
            for pattern in replacement_patterns:
                body = re.sub(pattern, sender_name, body, flags=re.IGNORECASE)
            
            logging.info(f"Email content generated for {email}")
            return {"subject": subject, "body": body}
        else:
            logging.error(f"OpenAI API error: {response.status_code}, {response.text}")
            # Return a fallback email if OpenAI API fails
            return {
                "subject": f"Custom Presentation Services for {name}",
                "body": f"Dear {name},\n\nI hope this message finds you well. My name is {sender_name}, and I am a professional presentation designer.\n\nI noticed you've been exploring our presentation services at SlideTeam. I'd love to discuss how we can help you create impactful presentations tailored to your specific needs.\n\nPlease feel free to reach out to me at +1-408-215-1583 to discuss further.\n\nBest regards,"
            }
    
    except Exception as e:
        logging.error(f"Error generating email content: {str(e)}")
        # Return a fallback email if any exception occurs
        return {
            "subject": f"Custom Presentation Services for {name}",
            "body": f"Dear {name},\n\nI hope this message finds you well. My name is {sender_name}, and I am a professional presentation designer.\n\nI noticed you've been exploring our presentation services at SlideTeam. I'd love to discuss how we can help you create impactful presentations tailored to your specific needs.\n\nPlease feel free to reach out to me at +1-408-215-1583 to discuss further.\n\nBest regards,"
        }
# Function to send personalized email to a recipient
def send_personalized_email(sender_email, sender_password, sender_display_name, recipient_data):
    try:
        email_address = recipient_data["email"]  # Lowercase field name
        name = recipient_data["name"]  # Lowercase field name
        visited_urls = recipient_data["visited_urls"]  # Lowercase field name
        user_id = recipient_data["id"]
        
        # Generate personalized email content with sender's name
        email_content = generate_email_content(name, email_address, visited_urls, sender_display_name, sender_email)
        
        # Create a new message with alternative MIME type
        msg = MIMEMultipart('alternative')
        msg['From'] = f"{sender_display_name} <{sender_email}>"  # Format with display name
        msg['To'] = email_address
        msg['Subject'] = email_content["subject"]
        
        # Add CC field
        cc_address = "rahul.singla@slidetech.in"
        msg['Cc'] = cc_address
        
        # Plain text version
        plain_text = email_content["body"]
        part1 = MIMEText(plain_text, 'plain')
        
        # HTML version with hyperlinks
        html_content = convert_to_html(plain_text)
        part2 = MIMEText(html_content, 'html')
        
        # Attach both versions (HTML will be preferred if client supports it)
        msg.attach(part1)
        msg.attach(part2)
        
        # Connect to SMTP server
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender_email, sender_password)
        
        # Send the email to both primary recipient and CC recipient
        all_recipients = [email_address]
        server.sendmail(sender_email, all_recipients, msg.as_string())
        server.quit()
        
        # Log success with truncated subject for readability
        truncated_subject = email_content["subject"][:40] + "..." if len(email_content["subject"]) > 40 else email_content["subject"]
        logging.info(f"Personalized HTML email sent from {sender_display_name} <{sender_email}> to {email_address} with CC to {cc_address}")
        logging.info(f"Subject: {truncated_subject}")
        
        # Update the email_sent status to 1 (sent)
        update_email_sent_status(user_id, 1)
        
        return True
    
    except Exception as e:
        logging.error(f"Failed to send personalized email to {email_address}: {str(e)}")
        
        # Update the email_sent status to indicate an error (you can use a different code than 0 if needed)
        # For now, we'll leave it as 0 so it will be retried next time
        
        return False

# Function to distribute emails evenly among senders
def distribute_recipients(recipients, sender_list):
    # Divide the recipients equally among all senders
    num_senders = len(sender_list)
    recipient_batches = [recipients[i::num_senders] for i in range(num_senders)]
    
    return recipient_batches

# Function to schedule the task
def schedule_task():
    # Fetch users that haven't been sent emails yet
    user_data_list = get_unsent_user_data()

    if not user_data_list:
        logging.info("No users to send emails to.")
        return

    # Distribute recipients to the senders
    sender_list = list(senders.items())  # Convert the dictionary to a list of tuples (email, details)
    recipient_batches = distribute_recipients(user_data_list, sender_list)

    # Track the total number of emails sent
    emails_sent = 0

    # Send personalized emails in batches
    for i, (sender_email, sender_details) in enumerate(sender_list):
        batch = recipient_batches[i]  # Get the batch for the current sender
        if batch:  # Only proceed if there are recipients in this batch
            for recipient_data in batch:
                # Send personalized email to each recipient
                if send_personalized_email(
                    sender_email, 
                    sender_details["password"], 
                    sender_details["display_name"], 
                    recipient_data
                ):
                    emails_sent += 1
                
                # Add a small delay between emails to avoid rate limiting
                time.sleep(1)

    # Log the total emails sent
    logging.info(f"Total emails sent in this batch: {emails_sent}")

# Start the scheduled task
logging.info("Starting scheduled task with OpenAI personalized emails. The first task will run immediately.")
schedule_task()

# Schedule the task to run every 2 minutes
schedule.every(30).minutes.do(schedule_task)

# Keep the script running for scheduled execution
while True:
    schedule.run_pending()
    time.sleep(1)  # wait for 1 second before checking again