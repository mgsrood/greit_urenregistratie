import requests
import pandas as pd
from datetime import datetime, timedelta
import pandas_gbq
import json
import os
from google.cloud import bigquery
from dotenv import load_dotenv

# Load .env
load_dotenv()

# Define Get Data From BigQuery
def get_data_from_bigquery(project_id, dataset_id, table_id):
    
    # Make a BigQuery client
    client = bigquery.Client(project=project_id)

    # Build the reference to the dataset
    table_ref = client.dataset(dataset_id).table(table_id)

    # Get the table
    table = client.get_table(table_ref)

    # Load the data into a DataFrame
    df = client.list_rows(table).to_dataframe()

    return df

# Get the BigQuery keys
gc_keys = os.getenv("GREIT_GOOGLE_CREDENTIALS")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gc_keys

# BigQuery information
project_id = os.getenv("UREN_REGISTRATIE_PROJECT_ID")
dataset_id = os.getenv("UREN_REGISTRATIE_DATASET_ID")
table_id = os.getenv("UREN_REGISTRATIE_TABLE_ID")
full_table_id = f'{project_id}.{dataset_id}.{table_id}'

# Get the BigQuery data
dataframe = get_data_from_bigquery(project_id, dataset_id, table_id)
existing_id_list = dataframe['ID']

# Make a BigQuery Client
bigquery_client = bigquery.Client(project=project_id)

# Keeping ID
id = os.environ.get('KEEPING_ID')

# Base url
url = os.getenv('KEEPING_BASE_URL')
full_url = f"{url}{id}/"

# Authorization
access_token = os.environ.get('KEEPING_ACCESS_TOKEN')
headers = {
'Authorization': f'Bearer {access_token}',
'Accept': 'application/json'
}

# Projects GET request
response = requests.request("GET", f"{full_url}projects", headers=headers)
data = response.json()

# Expand the projects data
projects = data['projects']
df_data = []

# Iterate over the projects data
for project in projects:
    df_data.append({
        'Project ID': project['id'],
        'Project': project['name'],
        'Klant ID': project['client']['id'] if project['client'] else None
    })

# Make a projects df
df_projects = pd.DataFrame(df_data)

# Tasks GET request
response = requests.request("GET", f"{full_url}tasks", headers=headers)
data = response.json()

# Expand the tasks data
tasks = data['tasks']
df_data = []

# Iterate over the tasks data
for task in tasks:
    df_data.append({
        'Taak ID': task['id'],
        'Taak': task['name']
    })

# Make a tasks df
df_tasks = pd.DataFrame(df_data)

# Client GET request
response = requests.request("GET", f"{full_url}clients", headers=headers)
data = response.json()

# Expand the client data
clients = data['clients']
df_data = []

# Iterate over the tasks data
for client in clients:
    df_data.append({
        'Klant ID': client['id'],
        'Klant': client['name']
    })

# Make a tasks df
df_clients = pd.DataFrame(df_data)

# Function to get tomorrow and 90 days ago
def get_tomorrow_and_90_days_ago():
    today = datetime.now()
    tomorrow = today + timedelta(days=1)
    ninety_days_ago = today - timedelta(days=90)
    return tomorrow.strftime('%Y-%m-%d'), ninety_days_ago.strftime('%Y-%m-%d')

tomorrow, ninety_days_ago = get_tomorrow_and_90_days_ago()

# Variables
start_date = ninety_days_ago
end_date = tomorrow

# Time entries GET request
next_page = True
page = 1
df_data = []

while next_page:
    response = requests.request("GET", f"{full_url}report/time-entries?page={page}&from={start_date}&to={end_date}", headers=headers)

    # Parsing the request to json
    data = response.json()

    # Expand the data
    time_entries = data['time_entries']

    # Iterate over the data
    for entry in time_entries:
        # Turn date into a datetime object
        date = datetime.strptime(entry['date'], '%Y-%m-%d').date()

        df_data.append({
            'Datum': date,
            'Uren': entry['hours'],
            'Klant ID': entry['project_id'],
            'Project ID': entry['project_id'],
            'Taak ID': entry['task_id'],
            'ID': entry['id']
        })
    
    # Check if it's the last page
    if data['meta']['current_page'] == data['meta']['last_page']:
        break

    # Move to the next page
    page += 1

# Make a Dataframe
df_time_entries = pd.DataFrame(df_data)

# Merg the DataFrames
df_pre_merged = pd.merge(df_projects, df_clients, on='Klant ID', how='left')
df_merged = pd.merge(df_time_entries, df_pre_merged, on='Project ID', how='left')
df_merged = pd.merge(df_merged, df_tasks, on='Taak ID', how='left')

# Drop unnecessary columns
df_merged.drop(['Klant ID_x', 'Project ID', 'Taak ID', 'Klant ID_y'], axis=1, inplace=True)

# Change data types
df_merged['Datum'] = pd.to_datetime(df_merged['Datum'])

# Filter out existing IDs
df_to_upload = df_merged[~df_merged['ID'].isin(existing_id_list)]

# Insert data into newly made table
pandas_gbq.to_gbq(df_to_upload, full_table_id, project_id=project_id, if_exists='append')
print(f"Data is succesvol geüpload naar {full_table_id}.")