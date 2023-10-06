import mygeotab

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

api = mygeotab.API(username, password, database)
api.authenticate()

devices = api.get("Device", resultsLimit=1000)
trips = api.get("Trip", resultsLimit=1000)

print(trips)
