# production-engineering

This repo contains packages and scripts for petroleum engineering 🛢️ automation, optimization 🔬, and visualization 🖥️.

## [critical-lift.py](https://github.com/tysontrail/production-engineering/blob/0f1636a0b2b1e2d41553a3088c6d3ba2ea3318c8/critical-lift.py) 🗜️

- This module provides functions for calculating gas properties and critical flow rates for natural gas wells to unload water using empirical correlations

## [multi-select.py](https://github.com/tysontrail/production-engineering/blob/main/multi-select.py) 📊

- This module provides function scripts for extension functions and a property change event handler for selecting multiple cells in an Ignition Power Table column for bulk value updates.

## [nocodb-loader.py](https://github.com/tysontrail/production-engineering/blob/main/nocodb-loader.py) 💾

- This module provides utilities for updating a NocoDB database using data from a CSV file.

## [ignition-well-data.py](ignition-well-data.py) 🔥

- This module serves as a handler for HTTP POST requests to fetch well meter data. It is designed to work within an Ignition environment and interacts with the Ignition system's database and tag history. The handler processes incoming requests, extracts the necessary parameters, fetches data for each specified Unique Well Identifier (UWI), and returns the data in a JSON format.

## [geotab.py](geotab.py)🚚

- This Python module integrates with the Geotab API to fetch and process data related to vehicle trips, drivers, and groups. The script performs data extraction, transformation, and loading (ETL) operations to prepare the data for analysis. It primarily uses the pandas library for data manipulation, along with the json module for handling JSON data structures.
