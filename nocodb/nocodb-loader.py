"""
Module: nododb-loader.py

Summary:
This module provides utilities for updating a NocoDB database using data from a CSV file.
The main operations include:
1. Reading configurations from a specified file (defaulted to "config.txt").
2. Loading data from a given CSV file path.
3. Establishing a connection to a server specified in the configuration.
4. Fetching existing data from NocoDB to create a mapping of GDC_UWI to rowID.
5. Iterating through the loaded CSV data to update the NocoDB entries based on a specified mapping.

Key Functions:
- get_config_value(key, filename="config.txt"): Fetches a configuration value for a given key from the provided configuration file.

Usage:
To use the script, make sure the "config.txt" file is populated with the required configurations.
Once set up, run the script to update the NocoDB database with the data from the CSV file.

Note:
Ensure that the server, API key, and other sensitive details are kept confidential and are not hard-coded within the script.
"""

__author__ = "Tyson Trail"
__version__ = "1.0.0"
__maintainer__ = "Tyson Trail"

import http.client
import pandas as pd
import json

# -----------------
# Helper Functions
# -----------------


# Helper function to fetch configuration values from config file
def get_config_value(key, filename="config.txt"):
    """
    Fetches a configuration value for a given key from the provided configuration file.

    Args:
    - key (str): The key whose value is to be fetched.
    - filename (str, optional): The name of the configuration file. Defaults to "config.txt".

    Returns:
    - str: The corresponding value of the given key.

    Raises:
    - ValueError: If the key is not found in the configuration file.
    """
    with open(filename, "r") as file:
        for line in file:
            if line.startswith(key):
                return line.split("=")[1].strip()
    raise ValueError(f"{key} not found in configuration file.")


# -------------------------------------
# Loading Configuration and CSV Data
# -------------------------------------

# Load spreadsheet data
csv_path = get_config_value("CSV_PATH")
df = pd.read_csv(csv_path)

# Testing only the first 100 rows
# df = df.head(600)

# Connection details and headers
server_address = get_config_value("SERVER_ADDRESS")
conn = http.client.HTTPConnection(server_address)
headers = {"xc-auth": get_config_value("API_KEY"), "Content-Type": "application/json"}

# A dictionary to map the last value of the tag path to its corresponding column in NocoDB
column_mapping = {
    "available": "AVAILABILITY",
    "status": "STATUS",
    "eto_date": "ETO_DATE",
    "notes": "NOTES",
}

api_base_path = get_config_value("API_BASE_PATH")
table_path = get_config_value("TABLE_PATH")
view_path = get_config_value("VIEW_PATH")

# -------------------------------------
# Fetching Existing Data from NocoDB
# -------------------------------------

# Fetch all rows from the table to create a mapping of GDC_UWI to rowID
conn.request("GET", f"{api_base_path}{table_path}{view_path}?l=*", headers=headers)
res = conn.getresponse()
if res.status == 200:
    response_content = res.read()
    # print(f"API Response: {response_content}")
    rows_data = json.loads(response_content)
    uwi_to_rowID = {
        row["GDC_UWI"]: row["Id"]
        for row in rows_data["list"]
        if "GDC_UWI" in row and "Id" in row
    }
    print(f"Found {len(uwi_to_rowID)} entries in NocoDB.")
else:
    print(f"Error fetching data. Status Code: {res.status}")
    print(res.read())
    # Exit the script if the request fails
    exit(1)

# --------------------------
# Updating Data in NocoDB
# --------------------------

# Iterate through rows in the spreadsheet
for index, row in df.iterrows():
    gdc_uwi = row["GDC_UWI"]

    # Skip the row if the UWI is empty or null
    if not gdc_uwi or pd.isnull(gdc_uwi):
        print(f"Skipping row {index + 1} due to missing UWI.")
        continue

    # Extract the tag path
    tag_path = row["TAGPATH"]

    # Determine the column in NocoDB to update
    last_value = tag_path.split("/")[-1]
    column_to_update = column_mapping.get(last_value)

    if not column_to_update:
        print(f"Unknown tag path value: {last_value}. Skipping...")
        continue

    update_data = {column_to_update: tag_path}
    print(f"Updating entry with UWI: {gdc_uwi} with data: {update_data} . . . ", end="")

    # Check if the UWI exists in NocoDB
    row_id = uwi_to_rowID.get(gdc_uwi)

    # If the UWI exists, update the entry
    if row_id:
        # Use rowID to update the specific entry in NocoDB
        conn.request(
            "PATCH",
            f"{api_base_path}{table_path}{view_path}/{row_id}",
            headers=headers,
            body=json.dumps(update_data),
        )
        # print(f"Request Body: {json.dumps(update_data)}")
        update_res = conn.getresponse()
        update_data = update_res.read()  # Consumes the response data

        if update_res.status != 200:
            print(
                f"Failed to update entry with rowID: {row_id}. Server Response: {update_data}"
            )
        else:
            print(f"Complete")
    else:
        print(f"No entry found with UWI: {gdc_uwi}")

# Close the connection
conn.close()
