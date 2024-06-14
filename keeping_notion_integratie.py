import requests
import json
import os
import pandas as pd
from datetime import datetime

from dotenv import load_dotenv

# Load .env
load_dotenv()

# Base URL
url = os.getenv("NOTION_API_URL")

# Authorization
access_token = os.getenv("NOTION_API_TOKEN")
notion_headers = {
    "Authorization": "Bearer " + access_token,
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}
database_id = os.getenv("NOTION_DATABASE_ID")

# Data extraction function
def get_pages(num_pages=None):
    """
    If num_pages is None, get all pages, otherwise just the defined number.
    """
    url = f"https://api.notion.com/v1/databases/{database_id}/query"

    get_all = num_pages is None
    page_size = 100 if get_all else num_pages

    payload = {"page_size": page_size}
    response = requests.post(url, json=payload, headers=notion_headers)

    data = response.json()

    results = data["results"]

    while data["has_more"] and get_all:
        payload = {"page_size": page_size, "start_cursor": data["next_cursor"]}
        url = f"https://api.notion.com/v1/databases/{database_id}/query"
        response = requests.post(url, json=payload, headers=notion_headers)
        data = response.json()
        results.extend(data["results"])

    return results

# Run function and create an empty data list
pages = get_pages()
data_list = []

# Iterate over the data rows
for page in pages:
    page_id = page["id"]
    props = page["properties"]
    id = props["ID"]["title"][0]["text"]["content"]
    client = props["Klant"]["rich_text"][0]["text"]["content"]
    project = props["Project"]["rich_text"][0]["text"]["content"]
    task = props["Taak"]["rich_text"][0]["text"]["content"]
    date = props["Datum"]["date"]["start"]
    hours = props["Uren"]["number"]

    data_list.append({"ID": id, "Client": client, "Project": project, "Task": task, "Date": date, "Hours": hours})

# Create a DataFrame
df = pd.DataFrame(data_list)

# Extract the IDs
if not df.empty:
    list = df['ID']
    existing_id_list = list.astype(int)
else:
    existing_id_list = []


# Keeping ID
id = os.getenv('KEEPING_ID')

# Base url
url = os.getenv('KEEPING_BASE_URL')
full_url = f"{url}{id}/"

# Authorization
access_token = os.getenv('KEEPING_ACCESS_TOKEN')
keeping_headers = {
'Authorization': f'Bearer {access_token}',
'Accept': 'application/json'
}

# Projects GET request
response = requests.request("GET", f"{full_url}projects", headers=keeping_headers)
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
response = requests.request("GET", f"{full_url}tasks", headers=keeping_headers)
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
response = requests.request("GET", f"{full_url}clients", headers=keeping_headers)
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

# Variables
start_date = os.getenv('START_DATE')
end_date = os.getenv('END_DATE')

# Time entries GET request
next_page = True
page = 1
df_data = []

while next_page:
    response = requests.request("GET", f"{full_url}report/time-entries?page={page}&from={start_date}&to={end_date}", headers=keeping_headers)

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

# Writing endpoint for Notion
endpoint = f"https://api.notion.com/v1/pages/"

# Iterate over DataFrame rows
if df_to_upload.empty:
    print("Geen nieuwe gegevens om te uploaden naar Notion.")
else:
    for index, row in df_to_upload.iterrows():
        # Details of the new rows
        new_row_data = {
            "parent": {"database_id": database_id},
            "properties": {
                "ID": {"title": [{"text": {"content": str(row['ID'])}}]},
                "Datum": {"date": {"start": row['Datum'].strftime("%Y-%m-%d")}},
                "Uren": {"number": row['Uren']} if not pd.isna(row['Uren']) else {"number": 0},
                "Klant": {"rich_text": [{"text": {"content": row['Klant']}}]} if not pd.isna(row['Klant']) else {"rich_text": [{"text": {"content": "Algemeen"}}]},
                "Project": {"rich_text": [{"text": {"content": row['Project']}}]},
                "Taak": {"rich_text": [{"text": {"content": row['Taak']}}]}
            }
        }
        
        # Send a POST-request to write new data
        response = requests.post(endpoint, headers=notion_headers, json=new_row_data)

        # Check the status
        if response.status_code == 200:
            print(f"Rij {index + 1} succesvol toegevoegd aan de Notion Database.")
        else:
            print(f"Fout bij het toevoegen van rij {index + 1}:", response.text)
