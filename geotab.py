import mygeotab
import pandas as pd
import json

# -----------------
# Helper Function
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
# Loading Configuration and Geotab API
# -------------------------------------

# Load username, password, and database data
username = get_config_value("GEOTAB_USERNAME")
password = get_config_value("GEOTAB_PASSWORD")
database = get_config_value("GEOTAB_DATABASE")

# Authenticate API
api = mygeotab.API(username, password, database)
api.authenticate()

# Get all devices, trips, and drivers
# devices = api.get("Device", resultsLimit=1)
trips = api.get("Trip", resultsLimit=100, fromDate="2023-10-10", toDate="2023-10-12")
drivers = api.get("User", resultsLimit=100)
groups = api.get("Group", resultsLimit=100)

# Convert to pandas dataframes
# devices = pd.DataFrame(devices)
trips = pd.DataFrame(trips)
drivers = pd.DataFrame(drivers)
print(drivers.head())
groups = pd.DataFrame(groups)


# Extract the 'id' from the 'Driver' column
def get_driver_id(driver_info):
    # Check if driver_info is a dictionary and has an 'id' key
    if isinstance(driver_info, dict):
        return driver_info.get(
            "id", None
        )  # Get 'id' if it exists, otherwise return None
    # Check if driver_info is a string representation of a dictionary
    elif isinstance(driver_info, str) and driver_info.startswith("{"):
        try:
            # Try to parse the string as JSON
            driver_dict = json.loads(driver_info)
            return driver_dict.get("id", None)
        except json.JSONDecodeError:
            # Handle the case where the string isn't valid JSON
            return None
    else:
        # If it's a string but not a dict representation, or some other type, return None or handle differently
        return None


# Apply the function to each element in the 'driver' column
trips["driverid"] = trips["driver"].apply(get_driver_id)

# Drop the original 'Driver' column
trips = trips.drop("driver", axis=1)

# Transform drivers
# Explode the driverGroups column
drivers = drivers.explode("driverGroups")

# Extract the number from the dictionaries in 'driverGroups'
drivers["driverGroupIDs"] = drivers["driverGroups"].apply(
    lambda x: x["id"] if isinstance(x, dict) and "id" in x else x
)

# Drop the original 'driverGroups' column
drivers = drivers.drop("driverGroups", axis=1)

# Export to CSV
# devices.to_csv("devices.csv")
trips.to_csv("trips.csv")
drivers.to_csv("drivers.csv")
# groups.to_csv("groups.csv")

# Print column names
# print(f"Device Columns: {devices.columns}")
# print(f"Trip Columns: {trips.columns}")
# print(f"Driver Columns: {drivers.columns}")
# print(f"Group Columns: {groups.columns}")

# Adjust display options
# pd.set_option("display.max_rows", None)
# pd.set_option("display.max_columns", None)

# print(drivers.head(10))

# print(f"Driver Columns: {drivers.columns}")

# for device in devices:
#     print(device["name"])
#     print(device["groups"])

# for index, driver in enumerate(drivers):
#     driver_name = driver["name"]
#     driver_groups = driver.get("driverGroups", "No groups")
#     print(f"{index + 1}. {driver_name} {driver_groups}")
