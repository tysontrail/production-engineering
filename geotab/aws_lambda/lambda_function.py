import os
import mygeotab
import pandas as pd
import json
import datetime


def lambda_handler(event, context):
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
      - Gets start and end date from JSON body

    - Data Extraction:
      - Fetches data related to trips, drivers, and groups from the Geotab API.
      - Converts the fetched data into pandas DataFrames for ease of manipulation.

    - Data Transformation:
      - Processes the trips DataFrame by extracting driver IDs using the get_driver_id function
        and removing unnecessary columns.
      - Transforms the drivers DataFrame by exploding the driverGroups column and extracting group IDs.
      - Performs necessary data cleaning and formatting operations.

    - Data Merging:
      - Merges the trips DataFrame with the drivers DataFrame based on driver IDs.
      - Further merges the resulting DataFrame with the groups DataFrame based on group IDs.

    - Final Data Cleaning:
      - Renames columns for clarity and readability.
      - Drops a significant number of unnecessary columns to streamline the dataset.

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

    # -----------------
    # Helper Functions
    # -----------------

    # Helper function to fetch configuration values from a file
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

    # -------------------------------------
    # Loading Configuration and Geotab API
    # -------------------------------------

    # Load username, password, and database data
    username = os.environ["geotab_username"]
    password = os.environ["geotab_password"]
    database = os.environ["geotab_database"]

    # Authenticate API
    api = mygeotab.API(username, password, database)
    api.authenticate()

    # Parsing the JSON body from the event
    body = json.loads(event.get("body", "{}"))

    # Extracting start_date and end_date
    start_date_str = body.get("start_date")
    end_date_str = body.get("end_date")

    # Convert string dates to datetime.date objects
    start_date = (
        datetime.datetime.strptime(start_date_str, "%Y-%m-%d").date()
        if start_date_str
        else None
    )
    end_date = (
        datetime.datetime.strptime(end_date_str, "%Y-%m-%d").date()
        if end_date_str
        else None
    )

    # Validate start_date and end_date
    if start_date is None or end_date is None:
        return {
            "statusCode": 400,
            "body": json.dumps("Invalid or missing start_date/end_date"),
        }

    # Get trips data
    trips = fetch_data_in_chunks(api, start_date, end_date)

    # Get drivers and groups data
    drivers = api.get("User", resultsLimit=None)
    groups = api.get("Group", resultsLimit=None)

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
    # trips.to_csv("trips.csv")
    # drivers.to_csv("drivers.csv")
    # groups.to_csv("groups.csv")

    # Export to json
    return {"statusCode": 200, "body": json.dumps({"data": trips.to_json()})}
