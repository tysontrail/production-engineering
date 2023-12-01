"""
Module Summary: Geotab Data Processing

Description:
This Python module integrates with the Geotab API to fetch and process data related to vehicle trips,
drivers, and groups. The script performs data extraction, transformation, and loading (ETL) operations
to prepare the data for analysis. It primarily uses the pandas library for data manipulation, along
with the json module for handling JSON data structures.

Key Components:

Helper Functions:
1. get_config_value:
   - Fetches configuration values (like username, password, and database) from a specified
     configuration file.
   - Inputs: Configuration key and optional filename.
   - Outputs: Corresponding configuration value as a string.

2. fetch_data_in_chunks:
    - Fetches data in chunks from a specified API endpoint between start and end dates.
    - Inputs: API object, start date, end date, and optional chunk size.
    - Outputs: List of data fetched in chunks from the API.

3. get_driver_id:
   - Extracts the driver's ID from a dictionary or string representation of a dictionary.
   - Handles cases where the driver information is either a dictionary or a JSON string.
   - Returns the driver's ID or None if not found.

Main Script:
- Configuration and API Authentication:
  - Reads the Geotab API credentials and database information from a configuration file.
  - Authenticates with the Geotab API.

- Data Extraction:
  - Fetches data related to trips, drivers, devices, and groups from the Geotab API.
  - Converts the fetched data into pandas DataFrames for ease of manipulation.

- Data Transformation:
  - Processes the trips DataFrame by extracting driver and device IDs using the get_id function
    and removing unnecessary columns.
  - Transforms the devices DataFrame by exploding the deviceGroups column and extracting group IDs.
  - Performs necessary data cleaning and formatting operations.

- Data Merging:
  - Merges the devices DataFrame with the trips DataFrame based on device IDs.
  - Merges the trips DataFrame with the drivers DataFrame based on driver IDs.
  - Further merges the resulting DataFrame with the groups DataFrame based on group IDs.

- Final Data Cleaning:
  - Renames columns for clarity and readability.

- Exporting Data:
  - Exports the final processed DataFrame to a CSV file named 'trips.csv'.

Usage:
The script is intended to be run as a standalone module to process data from the Geotab API. It requires
a 'config.txt' file with the necessary Geotab API credentials and database information.

Output:
The output is a cleaned and consolidated CSV file ('trips.csv') containing trip data with corresponding
driver and group information. This file is suitable for further analysis or reporting.

Note:
This module is designed specifically for integration with the Geotab API and assumes a particular data
structure returned by the API. Any changes in the API's data format may require modifications to the script.
"""

import mygeotab
import pandas as pd
import json
import datetime

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


def fetch_data_in_chunks(api, start_date, end_date, chunk_size_days=30):
    """
    Fetches data in chunks from a specified API endpoint between start and end dates.

    Args:
        api: The API object used for making requests.
        start_date (datetime.date): The start date for fetching data.
        end_date (datetime.date): The end date for fetching data.
        chunk_size_days (int, optional): The number of days to include in each data-fetching chunk. Defaults to 30.

    Returns:
        list: A list of data fetched in chunks from the API.
    """

    current_start_date = start_date
    all_trips = []

    while current_start_date < end_date:
        # Calculate the end date for the current chunk
        current_end_date = min(
            current_start_date + datetime.timedelta(days=chunk_size_days), end_date
        )

        # Fetch data for the current chunk
        chunk_of_trips = api.get(
            "Trip",
            fromDate=current_start_date.strftime("%Y-%m-%d"),
            toDate=current_end_date.strftime("%Y-%m-%d"),
        )
        all_trips.extend(chunk_of_trips)

        # Set the start date for the next chunk to one day after the current end date
        current_start_date = current_end_date + datetime.timedelta(days=1)

    return all_trips


# Helper function to extract id from info
def get_id(info):
    """
    Extracts the 'id' value from a dictionary or a string representation of a dictionary.

    Parameters:
    info (dict or str): The data that is expected to be either a dictionary
    or a string representation of a dictionary containing an 'id' key.

    Returns:
    str or None: The value associated with the 'id' key if present; otherwise, None.
    """

    # Check if driver_info is a dictionary and contains the 'id' key
    if isinstance(info, dict):
        # Retrieve 'id' from the dictionary, return None if 'id' is not present
        return info.get("id", None)

    # Check if driver_info is a string and appears to be a dictionary (starts with '{')
    elif isinstance(info, str) and info.startswith("{"):
        try:
            # Attempt to parse the string as JSON
            info_dict = json.loads(info)
            # Retrieve 'id' from the parsed dictionary, return None if 'id' is not present
            return info_dict.get("id", None)
        except json.JSONDecodeError:
            # Handle cases where the string is not valid JSON
            # This could be logged or handled in a specific way if needed
            return None

    # If info is neither a dictionary nor a string representation of a dictionary
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

    start_date = datetime.date(2023, 11, 20)
    end_date = datetime.date.today()

    # Get trips data
    trips = fetch_data_in_chunks(api, start_date, end_date)

    # Get drivers devices, and groups data
    drivers = api.get("User", resultsLimit=None)
    groups = api.get("Group", resultsLimit=None)
    devices = api.get("Device", resultsLimit=None)

    # Convert to pandas dataframes
    trips = pd.DataFrame(trips)
    drivers = pd.DataFrame(drivers)
    groups = pd.DataFrame(groups)
    devices = pd.DataFrame(devices)

    # -------------------------------------
    # Transforming Data
    # -------------------------------------

    # Transform trips table

    # Apply the get_id function to each element in the 'driver' column
    trips["driverID"] = trips["driver"].apply(get_id)

    # Drop the original 'Driver' column
    trips = trips.drop("driver", axis=1)

    # Apply the get_id function to each element in the 'device' column
    trips["deviceID"] = trips["device"].apply(get_id)

    # Drop the original 'device' column
    trips = trips.drop("device", axis=1)

    # Transform devices table

    # Explode the deviceGroups column
    devices = devices.explode("groups")

    # Extract the number from the dictionaries in 'driverGroups'
    devices["deviceGroupID"] = devices["groups"].apply(
        lambda x: x["id"] if isinstance(x, dict) and "id" in x else x
    )

    # Drop the original 'driverGroups' column
    devices = devices.drop("groups", axis=1)

    # Rename id columns to deviceID, driverID, and groupID
    devices = devices.rename(columns={"id": "deviceID"})
    drivers = drivers.rename(columns={"id": "driverID"})
    groups = groups.rename(columns={"id": "groupID"})

    # Export to CSV for data inspection
    # trips.to_csv("trips.csv")
    # drivers.to_csv("drivers.csv")
    # groups.to_csv("groups.csv")
    # devices.to_csv("devices.csv")

    # Print Column Names
    # print("Devices columns: ")
    # print(devices.columns)
    # print("Drivers columns: ")
    # print(drivers.columns)
    # print("Groups columns: ")
    # print(groups.columns)
    # print("Trips columns: ")
    # print(trips.columns)

    # Keep only the columns we need
    trips = trips[
        [
            "afterHoursDistance",
            "afterHoursDrivingDuration",
            "afterHoursEnd",
            "afterHoursStart",
            "afterHoursStopDuration",
            "averageSpeed",
            "distance",
            "drivingDuration",
            "engineHours",
            "idlingDuration",
            "isSeatBeltOff",
            "maximumSpeed",
            "nextTripStart",
            # "speedRange1",
            # "speedRange1Duration",
            # "speedRange2",
            # "speedRange2Duration",
            # "speedRange3",
            # "speedRange3Duration",
            "start",
            "stop",
            "stopDuration",
            "stopPoint",
            "workDistance",
            "workDrivingDuration",
            "workStopDuration",
            "id",
            "driverID",
            "deviceID",
        ]
    ]

    drivers = drivers[
        [
            # "driverGroups",
            # "keys",
            # "viewDriversOwnDataOnly",
            # "licenseProvince",
            # "licenseNumber",
            # "acceptedEULA",
            # "driveGuideVersion",
            # "wifiEULA",
            # "activeDashboardReports",
            # "bookmarks",
            # "activeFrom",
            # "activeTo",
            # "availableDashboardReports",
            # "cannedResponseOptions",
            # "changePassword",
            # "comment",
            # "companyGroups",
            # "mediaFiles",
            # "dateFormat",
            # "phoneNumber",
            # "displayCurrency",
            # "countryCode",
            # "phoneNumberExtension",
            # "defaultGoogleMapStyle",
            # "defaultMapEngine",
            # "defaultOpenStreetMapStyle",
            # "defaultHereMapStyle",
            # "defaultPage",
            # "designation",
            # "employeeNo",
            "firstName",
            # "fuelEconomyUnit",
            # "electricEnergyEconomyUnit",
            # "hosRuleSet",
            # "isYardMoveEnabled",
            # "isPersonalConveyanceEnabled",
            # "isExemptHOSEnabled",
            # "isAdverseDrivingEnabled",
            # "authorityName",
            # "authorityAddress",
            "driverID",
            # "isEULAAccepted",
            # "isNewsEnabled",
            # "isServiceUpdatesEnabled",
            # "isLabsEnabled",
            # "isMetric",
            # "language",
            # "firstDayOfWeek",
            "lastName",
            # "mapViews",
            "name",
            # "privateUserGroups",
            # "reportGroups",
            # "securityGroups",
            # "showClickOnceWarning",
            # "timeZoneId",
            # "userAuthenticationType",
            # "zoneDisplayMode",
            # "companyName",
            # "companyAddress",
            # "carrierNumber",
            # "lastAccessDate",
            # "isDriver",
            # "isEmailReportEnabled",
            # "featurePreview",
            # "isAutoAdded",
            # "activeDefaultDashboards",
            # "jobPriorities",
            # "maxPCDistancePerDay",
            # "accessGroupFilter",
        ]
    ]

    groups = groups[
        [
            # "isGlobalReportingGroup",
            # "children",
            # "color",
            # "comments",
            "groupID",
            "name",
            # "reference",
        ]
    ]

    devices = devices[
        [
            # "auxWarningSpeed",
            # "enableAuxWarning",
            # "enableControlExternalRelay",
            # "externalDeviceShutDownDelay",
            # "immobilizeArming",
            # "immobilizeUnit",
            # "isAuxIgnTrigger",
            # "isAuxInverted",
            # "accelerationWarningThreshold",
            # "accelerometerThresholdWarningFactor",
            # "brakingWarningThreshold",
            # "corneringWarningThreshold",
            # "enableBeepOnDangerousDriving",
            # "enableBeepOnRpm",
            # "engineHourOffset",
            # "isActiveTrackingEnabled",
            # "isDriverSeatbeltWarningOn",
            # "isPassengerSeatbeltWarningOn",
            # "isReverseDetectOn",
            # "isIoxConnectionEnabled",
            # "odometerFactor",
            # "odometerOffset",
            # "rpmValue",
            # "seatbeltWarningSpeed",
            # "activeFrom",
            # "activeTo",
            # "disableBuzzer",
            # "enableBeepOnIdle",
            # "enableSpeedWarning",
            # "engineType",
            # "idleMinutes",
            # "isSpeedIndicator",
            # "minAccidentSpeed",
            # "speedingOff",
            # "speedingOn",
            # "goTalkLanguage",
            # "fuelTankCapacity",
            # "autoGroups",
            # "customParameters",
            # "enableMustReprogram",
            # "engineVehicleIdentificationNumber",
            # "ensureHotStart",
            # "gpsOffDelay",
            "licensePlate",
            "licenseState",
            # "major",
            # "minor",
            # "parameterVersion",
            # "pinDevice",
            "vehicleIdentificationNumber",
            # "parameterVersionOnDevice",
            "comment",
            "timeZoneId",
            # "deviceType",
            "deviceID",
            # "ignoreDownloadsUntil",
            # "maxSecondsBetweenLogs",
            "name",
            # "productId",
            # "serialNumber",
            # "timeToDownload",
            # "workTime",
            # "customProperties",
            # "mediaFiles",
            # "deviceFlags",
            # "devicePlans",
            # "devicePlanBillingInfo",
            # "autoHos",
            # "customFeatures",
            # "disableSleeperBerth",
            # "wifiHotspotLimits",
            # "isContinuousConnectEnabled",
            # "obdAlertEnabled",
            "deviceGroupID",
        ]
    ]

    # -------------------------------------
    # Merging Data
    # -------------------------------------

    # Merge trips and drivers
    trips = trips.merge(drivers, left_on="driverID", right_on="driverID", how="left")

    # Merge trips and devices
    trips = trips.merge(devices, left_on="deviceID", right_on="deviceID", how="left")

    # Merge trips and groups
    trips = trips.merge(groups, left_on="deviceGroupID", right_on="groupID", how="left")

    # -------------------------------------
    # Cleaning Data
    # -------------------------------------

    # Rename columns
    trips = trips.rename(
        columns={
            "id": "tripID",
            "name_y": "driverName",
            "name_x": "email",
            "name": "groupName",
        }
    )

    # Print the unique values in groupName
    # print(trips["groupName"].unique())

    # Only keep rows where groupName is 'Field Hanlan', 'Field East', 'Field PRA', 'Field WCH', 'Field Ferrier', 'Field Gilby'
    trips = trips[
        trips["groupName"].isin(
            [
                "Field Hanlan",
                "Field East",
                "Field PRA",
                "Field WCH",
                "Field Ferrier",
                "Field Gilby",
                "Field Watelet",
            ]
        )
    ]

    # Check tripID column for duplicates
    # print(trips["tripID"].duplicated(keep=False).sum())

    # Slice the dataframe to only include rows where tripID is duplicated, used for debugging
    # trips = trips[trips["tripID"].duplicated(keep=False)].sort_values(by="tripID")

    # -------------------------------------
    # Exporting Data
    # -------------------------------------

    # Export to CSV
    trips.to_csv("trips.csv")

    # Export to json
    # trips = trips.to_json("trips.json")
