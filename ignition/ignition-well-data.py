"""
Module: Well Meter Data API Handler

Description:
This module serves as a handler for HTTP POST requests to fetch well meter data. 
It is designed to work within an Ignition environment and interacts with the 
Ignition system's database and tag history. The handler processes incoming requests,
extracts the necessary parameters, fetches data for each specified Unique Well Identifier (UWI),
and returns the data in a JSON format.

The module handles:
- Extraction and validation of request parameters.
- Construction of tag paths for querying tag history data.
- Fetching data for multiple UWIs within a specified time range.
- Conversion of Ignition datasets to a JSON format for HTTP response.

Functions:
- welldata(endTime, startTime, uwi): Fetches tag history data for a given UWI.
- datasetToJson(dataset): Converts an Ignition dataset to a JSON string.

Usage:
The module is intended to be used as a response handler for HTTP POST requests in 
an Ignition Web Development environment. It expects a JSON payload containing UWIs, 
tag names, and a time range.

Note:
The module is designed to be used within an Ignition environment and cannot be executed as a standalone script.
"""

__author__ = "Tyson Trail"
__version__ = "1.0.0"
__maintainer__ = "Tyson Trail"

import json
import datetime
import java

def welldata(endTime,startTime,uwi,tagNames):
    """
    Fetches data for a given UWI within the specified time range.

    Args:
        endTime (str): The end time for the data query.
        startTime (str): The start time for the data query.
        uwi (str): Unique Well Identifier.
        tagNames (list): Tag names to query.

    Returns:
        Dataset: The Ignition dataset containing the queried data.
    """
    tagpath_list = []  # Create a new list to store the constructed paths
    
    # Construct tag paths for each tag name
    for tag in tagNames:
        params = {"UWI": uwi, "Tagname": tag}
        tagpath_result = system.db.runNamedQuery("TagPath", params)

    # Assuming the result has at least one row
    # Extract siteID and compID from the query result
        siteID = tagpath_result.getValueAt(0, 1)
        compID = tagpath_result.getValueAt(0, 2)
        folderPath = '[default]'+siteID + '/' + compID + '/'
    
    # Construct the full tag path and add it to the list
        full_tagpath = folderPath + tag
        print(full_tagpath)
        tagpath_list.append(full_tagpath)

    # Query tag history data
    dataset = system.tag.queryTagHistory(
        paths=tagpath_list,
        startDate=startTime,
        endDate=endTime,
        returnSize=-1,
        returnFormat='Tall'
        )
    return dataset

def datasetToJson(dataset):
        """
        Convert an Ignition dataset to a JSON format.
    
        Args:
        dataset (Dataset): The Ignition dataset to convert.
    
        Returns:
        str: A JSON string representing the dataset.
        """
        data = []
        columnNames = list(dataset.getColumnNames())
        
        # Convert each row of the dataset to a dictionary
        for rowIndex in range(dataset.getRowCount()):
            rowDict = {}
            for colIndex, colName in enumerate(columnNames):
                value = dataset.getValueAt(rowIndex, colIndex)
                # Check if the value is a datetime object
                if isinstance(value, java.util.Date):
                    # Convert to a string in ISO 8601 format
                    value = system.db.dateFormat(value, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
                rowDict[colName] = value
            data.append(rowDict)

        return json.dumps(data)

def doPost(request, session):
	"""
	Respond to an incoming HTTP request. Formulate your response as a dictionary
	using any of the following keys:

	'html' HTML source as a string.

	'json' A python dictionary which will be encoded as 'application/json' data

	'file' A file path to send as the response.

	'bytes' A byte[] to send back. Mime type will be 'application/octet-stream'
	if not specified.

	'response' Any type of plain text response.

	'contentType' The mime type of the response. Needed only if ambiguous.

	Arguments:
		request: A dictionary with information about the incoming web request.
			context: A reference to the Gateway's context object.
			data: The data on the request. If the content type is application/json,
			      will be a Python structure (list or dictionary). If not, will either be
			      plain text or a raw byte array.
			headers (dict): A dictionary of header : value pairs. If multiple
			                values were returned for the same header, values will be in a tuple.
			params (dict): A dictionary of URL parameters. If multiple values were
			               returned for the same parameter, values will be in a tuple.
			remainingPath (str): The remainder of the URL after this resource.
			remoteAddr (str): Returns the IP address of the client.
			remoteHost (str): Returns the fully qualified name of the client.
			scheme (str): Returns the name of the scheme used to make this request,
			              i.e. 'http' or 'https'.
			servletRequest: The underlying Java HttpServletRequest object.
			servletResponse: The underlying Java HttpServletResponse object.
		session: A dictionary that will be persistent across multiple requests
		         from the same session. If authentication is required, will have a 'user'
		         attribute containing information about the authenticated user, and a
		         'retryAttempts' attribute with the number of attempts made.
	"""
		    		
	# Extract request data
	requestData = request['data']
	uwis = requestData['uwis'] # List of Unique Well Identifiers
	start_time = requestData['timeRange']['start'] # Start time for the data query
	end_time = requestData['timeRange']['end'] # End time for the data query
	tag_names = requestData['tagNames'] # List of tag names to query

	# Validate request data
	if not uwis or not tag_names or not start_time or not end_time:
	    return {'json': {"error": "Missing required data in request."}}

	# Collect data for all UWIs and convert each dataset to JSON, storing it in a dictionary
    uwiDataDict = {}
    for uwi in uwis:
        data = welldata(end_time, start_time, uwi, tag_names)
        if data is not None:
            uwiDataDict[uwi] = datasetToJson(data)    
            
    # Convert the entire dictionary to a JSON string
    combinedJsonData = json.dumps(uwiDataDict)

    # Return the combined JSON data as the response
    return { 'json': combinedJsonData}