import mygeotab
import pandas as pd

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
# trips = api.get("Trip", resultsLimit=1)
# drivers = api.get("User", resultsLimit=1)
groups = api.get("Group", resultsLimit=100)

# Convert to pandas dataframes
# devices = pd.DataFrame(devices)
# trips = pd.DataFrame(trips)
# drivers = pd.DataFrame(drivers)
groups = pd.DataFrame(groups)

# Print column names
# print(f"Device Columns: {devices.columns}")
# print(f"Trip Columns: {trips.columns}")
# print(f"Driver Columns: {drivers.columns}")
print(f"Group Columns: {groups.columns}")

# Adjust display options
pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)

print(groups)

# print(f"Driver Columns: {drivers.columns}")

# for device in devices:
#     print(device["name"])
#     print(device["groups"])

# for index, driver in enumerate(drivers):
#     driver_name = driver["name"]
#     driver_groups = driver.get("driverGroups", "No groups")
#     print(f"{index + 1}. {driver_name} {driver_groups}")
