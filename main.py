import requests
import psycopg2
from psycopg2 import sql
from datetime import datetime, date, timedelta
import csv
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging
import sys
from dotenv import load_dotenv



# Configure logging
logging.basicConfig(filename='/Users/ayushpathak/PycharmProjects/PythonProject6/cron_job.log', level=logging.INFO)

# Log the start of the script
logging.info(f'Script started at {datetime.now()}')

# Your existing code here

# Log the end of the script
logging.info(f'Script ended at {datetime.now()}')
# Zoho OAuth credentials
load_dotenv()

client_id = os.getenv('client_id')
client_secret = os.getenv('client_secret')
refresh_token = os.getenv('refresh_token')
db_host = os.getenv('db_host')
db_name = os.getenv('db_name')
db_user = os.getenv('db_user')
db_password = os.getenv('db_password')
db_port = os.getenv('db_port')

# Zoho token endpoint
token_url = "https://accounts.zoho.com/oauth/v2/token"

# Path to the CSV file
csv_file_path = os.path.join(os.path.dirname(__file__), 'monitor group mapping - Sheet1.csv')

# Load monitor group mappings from CSV file
monitor_group_mapping = {}
with open(csv_file_path, mode='r') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        monitor_id = row['Monitor ids']
        group_id = row['Group id']
        monitor_group_mapping[monitor_id] = group_id

def get_new_access_token():
    payload = {
        'client_id': client_id,
        'client_secret': client_secret,
        'refresh_token': refresh_token,
        'grant_type': 'refresh_token'
    }
    response = requests.post(token_url, data=payload)
    if response.status_code == 200:
        tokens = response.json()
        return tokens.get('access_token')
    else:
        print(f"Error refreshing token: {response.status_code} - {response.reason}")
        return None



retry_strategy = Retry(
    total=3,
    backoff_factor=0.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"]
)

adapter = HTTPAdapter(max_retries=retry_strategy)
http = requests.Session()
http.mount("https://", adapter)
http.mount("http://", adapter)


access_token = get_new_access_token()
if not access_token:
    print("Failed to obtain access token.")
    exit()

url = "https://www.site24x7.com/api/maintenance"
headers = {
    "Authorization": f"Zoho-oauthtoken {access_token}",
    "Accept": "application/json; version=2.0"
}

response = requests.get(url, headers=headers)
if response.status_code == 200:
    maintenance_schedules = response.json().get('data', [])
    conn = psycopg2.connect(
        host=db_host,
        database=db_name,
        user=db_user,
        password=db_password,
        port=db_port
    )
    cursor = conn.cursor()
    ct=0
    today_date = datetime.today().date()

    if len(sys.argv) > 1:
        input_date_str = sys.argv[1]
        try:
            input_date = datetime.strptime(input_date_str, '%Y-%m-%d').date()
        except ValueError:
            print("Invalid date format. Use YYYY-MM-DD.")
            exit()
    else:
        input_date = datetime.today().date()

    for maintenance_schedule in maintenance_schedules:
        ct+=1
        end_date_str = maintenance_schedule.get('end_date', 'N/A')
        start_date_str = maintenance_schedule.get('start_date', 'N/A')
        duration = maintenance_schedule.get('duration', 'N/A')
        display_name = maintenance_schedule.get('display_name', 'N/A')
        monitors = maintenance_schedule.get('monitors', [])


        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()

        # Lookup group IDs for monitor IDs
        group_ids = [monitor_group_mapping.get(str(monitor_id), 'Unknown') for monitor_id in monitors]
        if start_date == today_date:
            region = display_name
            if "Virginia" in display_name:
                region = "Asia-1 (N. Virginia)"
            elif "Singapore" in display_name:
                region = "Asia-2 (Singapore)"
            elif "Mumbai" in display_name:
                region = "Capillary-Private-1 (Mumbai)"
            elif "Ireland" in display_name:
                region = "EMEA(Ireland)"
            elif "US" in display_name:
                region = "US (Ohio)"

            print(f"region: {display_name}")
            print(f"Start Date: {start_date}")
            print(f"End Date: {end_date}")
            print(f"Duration: {duration} minutes")
            print(f"Monitors: {monitors}")
            print(f"Group IDs: {group_ids}")
            print("----------")

            insert_query = sql.SQL(
                """
                INSERT INTO public.downtime_maintenance (region, start_date, end_date, duration)
                VALUES (%s, %s, %s, %s)
                """
            )
            cursor.execute(insert_query, (region, start_date, end_date, duration))
    print(f"Total Maintenance count : {ct}")
    conn.commit()
    cursor.close()
    conn.close()

else:
    print(f"Error: {response.status_code} - {response.reason}")
