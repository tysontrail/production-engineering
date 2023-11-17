import mygeotab
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


# Helper function to extract driver id from driver info
def get_driver_id(driver_info):
    """
    Extracts the 'id' value from a dictionary or a string representation of a dictionary.

    Parameters:
    driver_info (dict or str): The data that is expected to be either a dictionary
    or a string representation of a dictionary containing an 'id' key.

    Returns:
    str or None: The value associated with the 'id' key if present; otherwise, None.
    """

    # Check if driver_info is a dictionary and contains the 'id' key
    if isinstance(driver_info, dict):
        # Retrieve 'id' from the dictionary, return None if 'id' is not present
        return driver_info.get("id", None)

    # Check if driver_info is a string and appears to be a dictionary (starts with '{')
    elif isinstance(driver_info, str) and driver_info.startswith("{"):
        try:
            # Attempt to parse the string as JSON
            driver_dict = json.loads(driver_info)
            # Retrieve 'id' from the parsed dictionary, return None if 'id' is not present
            return driver_dict.get("id", None)
        except json.JSONDecodeError:
            # Handle cases where the string is not valid JSON
            # This could be logged or handled in a specific way if needed
            return None

    # If driver_info is neither a dictionary nor a string representation of a dictionary
    else:
        # Return None or handle differently depending on requirements
        return None


if __name__ == "__main__":
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
    trips = api.get(
        "Trip", resultsLimit=100, fromDate="2023-10-10", toDate="2023-10-12"
    )
    drivers = api.get("User", resultsLimit=100)
    groups = api.get("Group", resultsLimit=100)

    # Convert to pandas dataframes
    trips = pd.DataFrame(trips)
    drivers = pd.DataFrame(drivers)
    groups = pd.DataFrame(groups)

    # -------------------------------------
    # Transforming Data
    # -------------------------------------

    # Transform trips table

    # Apply the get_driver_id function to each element in the 'driver' column
    trips["driverid"] = trips["driver"].apply(get_driver_id)

    # Drop the original 'Driver' column
    trips = trips.drop("driver", axis=1)

    # Transform drivers table

    # Explode the driverGroups column
    drivers = drivers.explode("driverGroups")

    # Extract the number from the dictionaries in 'driverGroups'
    drivers["driverGroupID"] = drivers["driverGroups"].apply(
        lambda x: x["id"] if isinstance(x, dict) and "id" in x else x
    )

    # Drop the original 'driverGroups' column
    drivers = drivers.drop("driverGroups", axis=1)

    # -------------------------------------
    # Merging Data
    # -------------------------------------

    # Merge trips and drivers
    trips = trips.merge(drivers, left_on="driverid", right_on="id", how="left")

    # Merge trips and groups
    trips = trips.merge(groups, left_on="driverGroupID", right_on="id", how="left")

    # -------------------------------------
    # Cleaning Data
    # -------------------------------------

    # Rename columns
    trips = trips.rename(
        columns={
            "id_x": "tripID",
            "name_x": "email",
            "name_y": "groupName",
        }
    )
    # Drop unnecessary columns
    trips = trips.drop(
        [
            "id_y",
            "keys",
            "viewDriversOwnDataOnly",
            "licenseProvince",
            "licenseNumber",
            "acceptedEULA",
            "driveGuideVersion",
            "wifiEULA",
            "activeDashboardReports",
            "bookmarks",
            "availableDashboardReports",
            "cannedResponseOptions",
            "changePassword",
            "comment",
            "companyGroups",
            "mediaFiles",
            "dateFormat",
            "phoneNumber",
            "phoneNumberExtension",
            "designation",
            "displayCurrency",
            "countryCode",
            "phoneNumber",
            "defaultGoogleMapStyle",
            "defaultMapEngine",
            "defaultOpenStreetMapStyle",
            "defaultHereMapStyle",
            "defaultPage",
            "employeeNo",
            "fuelEconomyUnit",
            "electricEnergyEconomyUnit",
            "hosRuleSet",
            "isYardMoveEnabled",
            "isPersonalConveyanceEnabled",
            "isExemptHOSEnabled",
            "isAdverseDrivingEnabled",
            "authorityName",
            "authorityAddress",
            "isEULAAccepted",
            "isNewsEnabled",
            "isServiceUpdatesEnabled",
            "isLabsEnabled",
            "isMetric",
            "language",
            "firstDayOfWeek",
            "privateUserGroups",
            "reportGroups",
            "securityGroups",
            "showClickOnceWarning",
            "timeZoneId",
            "userAuthenticationType",
            "zoneDisplayMode",
            "companyName",
            "companyAddress",
            "carrierNumber",
            "lastAccessDate",
            "isDriver",
            "isEmailReportEnabled",
            "featurePreview",
            "isAutoAdded",
            "activeDefaultDashboards",
            "jobPriorities",
            "maxPCDistancePerDay",
            "accessGroupFilter",
            "isGlobalReportingGroup",
            "children",
            "color",
            "comments",
            "id",
            "reference",
        ],
        axis=1,
    )

    print(trips.columns.values)
    # -------------------------------------
    # Exporting Data
    # -------------------------------------

    # Export to CSV
    trips.to_csv("trips.csv")
    # drivers.to_csv("drivers.csv")
    # groups.to_csv("groups.csv")
