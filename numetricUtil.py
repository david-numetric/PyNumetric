import jaydebeapi
import pandas as pd
import numpy as np
import math
import requests
from datetime import datetime, timedelta
import json
import logging
import dateutil
import time
import numpy as np

"""
First, pull configuration file in and define globals for use. This setup uses a config file to store things like
database credentials, table details, update values, etc. All is stored in a json file that is updated on each
incremental run.

"""
with open('config.json') as file:
    configData = json.load(file)

jclass = configData['database']['jclass']
connString = configData['database']['connstring']
jar = configData['database']['jar']
tables = pd.DataFrame(configData['tables'])
apiKey = configData['apikey']


def read_jdbc(sql):
    """
    Reads jdbc compliant data sources and returns a Pandas DataFrame. Stuffs ALL of the data into a single
    dataframe, though, so it's best used to read small tables, OR to read in the first 1000 rows to get
    table structure.

    uses jaydebeapi.connect and doc strings :-)
    https://pypi.python.org/pypi/JayDeBeApi/

    :param sql: SQL query to run.
    :return: Pandas DataFrame
    """

    global logger, jclass, connString, jar

    # First establish DB connection
    try:
        conn = jaydebeapi.connect(jclass, connString, jars=jar)
    except jaydebeapi.DatabaseError as de:
        logger.error('Database Connection Error.')
        logger.error(de)
        raise

    # now execute query against databasee
    try:
        curs = conn.cursor()
        curs.execute(sql)

        # Pull column names from the SQL cursor
        columns = [desc[0] for desc in curs.description]  # getting column headers

        # explicitly go and figure out datatypes, even though it turns out we really only need ints and floats here.
        # this is pretty hack-y, but it works to extract out arrays of the columns that fit each datatype

        # Start with empty lists
        integers = []
        strings = []
        floats = []
        dates = []

        # And fill lists with the datatypes. Not sure if this will work with all DB platforms
        for i in range(0, len(curs.description)):

            if(str(curs.description[i][1]).find("INTEGER")>0):
                integers.append(curs.description[i][0])
            elif(str(curs.description[i][1]).find("VARCHAR")>0):
                strings.append(curs.description[i][0])
            elif (str(curs.description[i][1]).find("DECIMAL")>0):
                floats.append(curs.description[i][0])
            elif (str(curs.description[i][1]).find("DATE")>0):
                dates.append(curs.description[i][0])

                df = pd.DataFrame(columns=columns)

        # This logic breaks the query into chunks rather than running one big one.
        while True:
            try:
                chunk = curs.fetchmany(1000)
                if not chunk:
                    break
                addDf = pd.DataFrame(chunk, columns=columns)
                df = df.append(addDf)

            # If there's an error here, it may just be a connectivity issue. So wait 60 seconds and try again.
            except jaydebeapi.DatabaseError as de:
                logger.error('Database Connection Error while doing incremental pull. Trying again in 60 seconds')
                time.sleep(60)
                try:
                    chunk = curs.fetchmany(1000)
                    if not chunk:
                        break
                    addDf = pd.DataFrame(chunk, columns=columns)
                    df = df.append(addDf)

                except jaydebeapi.DatabaseError as de:
                    logger.error('Still having a Database Connection Error. Aborting this particular attempt.')
                    logger.error(de)
                    raise
    except jaydebeapi.DatabaseError as de:
        logger.error('Database Connection Error.')
    finally:
        curs.close()
        conn.close()

    # this is where we manually force the integer and float columns to be numbers.
    df[integers] = df[integers].apply(pd.to_numeric)
    df[floats] = df[floats].apply(pd.to_numeric)

    return df


def addRows(df, tableid, chunkSize=1000):
    '''
    Adds all rows from a pandas dataframe, in batches of chunkSize, to the specified Numetric table

    Uses pandas and requests packages

    :param df: pandas dataframe containing data
    :param tableid: Numetric tableid of the target warehouse table
    :param apiKey: Numetric apiKey for your organization
    :param chunkSize: number of rows to send in each sequential API call. Default is 1000 rows.

    '''

    # logger and apiKey come from elsewhere.
    global logger, apiKey

    # Calculate number of iterations required to get through the dataset, given its length, and extract number of rows, too
    iterations = math.ceil(len(df) / chunkSize)
    nrows = len(df)

    # now iterate through [iterations] number of times, sending chunks of data each time
    for i in range(1, iterations + 1):  # +1 in range because of Python's iteration paradigm (excludes last item in range)
        # get starting and ending index numbers
        startNum = ((i - 1) * chunkSize)
        endNum = i * chunkSize

        # But also truncate when we go past nrows (happens on last chunk)
        if endNum > nrows + 1:
            endNum = nrows + 1

        # Subset dataframe and convert to appropriate JSON object
        thisChunk = df[startNum:endNum]
        dataToSend = thisChunk.to_json(orient='records')

        # Final assembly of the payload for the API call
        payload = "{\"rows\":" + dataToSend + "}"

        # Now initiate the API call

        # URL for adding rows to this table
        url = "https://api.numetric.com/v3/table/" + tableid + "/rows"

        # Incorporate the API Key for authorization
        headers = {'authorization': apiKey,
                   'Content-Type': "application/json"}

        # Make the API call, capturing the response code
        try:
            response = requests.request("POST", url, data=payload, headers=headers)
            # print(response.text)
            response.raise_for_status()
        # If there's an error here, it's usually a gateway error because one of the transmissions got lost.
        # Retrying will fix the issue.
        except requests.exceptions.HTTPError as e:
            # logger.error(e)
            logger.error("HTTP Error encountered. Retrying a few times...")
            time.sleep(30)
            for i in range(3):
                try:
                    response = requests.request("POST", url, data=payload, headers=headers)
                    response.raise_for_status()
                except requests.exceptions.HTTPError as e:
                    logger.error(e)
                    logger.error("API still not responding. Trying again.")
                    time.sleep(30)
        except requests.exceptions.RequestException as e:
            logger.error("Connection Error encountered.")
            logger.error(e)

        # If we're on the last iteration, report that it was successful.
        if (iterations == 1) or (i == iterations):
            logger.info("100% processed, sending command to process new data.")
            sendIndexCommand(tableid)
        # Otherwise, report progress.
        else:
            logger.info("%s percent processed, starting with row %s", str(round(endNum / nrows * 100)), str(endNum))




def sendIndexCommand(tableid):
    """
    Send the command to publish/index, given the tableid

    Uses requests packages

    :param tableid: Numetric tableid of the target warehouse table
    """

    global apiKey

    try:
        url = "https://api.numetric.com/v3/table/" + tableid + "/publish"
        headers = {'authorization': apiKey}
        response = requests.request("GET", url, headers=headers)
    except requests.exceptions.HTTPError as e:
        logger.error("HTTP Error encountered. Retrying a few times...")
        raise


def cleanTable(df, databaseTableName):
    """
    Applies cleaning operations for given databaseTableName to all rows in a pandas dataframe (df).

    Uses pandas package

    :param df: pandas dataframe containing data
    :param databaseTableName: Name of database table from which data originates. This is used to
        specify the appropriate cleaning operations

    """

    # Numetric doesn't deal well with Python's "None" values. So replace with empty strings
    df = df.replace([None], [""])

    # You can setup specific table cleaning here, given the db table name.
    # if (databaseTableName == 'SomeTable'):
    #     do some cleaning

    return df



def updateRows(tableid, databaseTableName, updateColumn, updateValue, chunkSize=1000):
    """
    Pulls new data from a given database table, cleans, and then pushes the data to Numetric

    :param tableid: Numetric tableid of the target warehouse table
    :param databaseTableName: Name of database table from which data originates
    :param updateColumn: Column name of the update column in the database table
    :param updateValue: last used update value for update logic. Works with version number might need some
                        tweaking for dates
    :param chunkSize: default is 1000, but this specifies the size of chunks that are both pulled and pushed

    :return: new update value derived from the data that was pulled to do the update
    """

    global logger, jclass, connString, jar, apiKey

    # Announce logic for update, mostly for troubleshooting
    logger.info("Pulling data from %s with update value >= %s.", databaseTableName, updateValue)

    # Build where command using the updateValue
    cmd = " WHERE " + updateColumn + " >= \'" + updateValue + "\'"

    # use readAndPush to efficiently pull and push data, given where command. Returns the new maximum
    newUpdateValue = readAndPush(cmd, databaseTableName, tableid, updateColumn=updateColumn, chunkSize=chunkSize)

    # push new update value back
    return newUpdateValue


def getTableId(tableName):
    """
    Searches for a Numetric table with the provided name, returning the
    corresponding table id if found, and otherwise returning 'NA'.

    Uses pandas and requests packages

    :param tableName: The desired Numetric table name
    :return: tableid of requested tablename if found. Otherwise "NA"

    """

    global logger, apiKey

    logger.info('Fetching tableID for table %s with API key %s', tableName, apiKey)

    # construct simple API call to get list of available tables
    url = "https://api.numetric.com/v3/table"

    headers = {'authorization': apiKey}

    response = requests.request("GET", url, headers=headers)

    try:
        response = requests.request("GET", url, headers=headers)
    except requests.exceptions.RequestException as e:
        logger.error(e)
        logger.error('API Connection Error. Could not get list of tables.')

    # if the response is empty, there are no tables.
    if response.text == '[]':
        return 'NA'
    else:
        # convert response to a dataframe because I like them better
        results = pd.read_json(response.text)

        # If the search result subset is non zero, then it found the name and can return the associated tableid
        if len(results[results['name'] == tableName]['id']) > 0:
            tableid = results[results['name'] == tableName]['id'].item()
            logger.info('Found table %s with id %s.', tableName, tableid)
            return tableid

        # otherwise, return 'NA' because the table name isn't found in the list of tables returned
        else:
            logger.info('Could not find table %s. Returning NA', tableName)
            return 'NA'


def readAndPush(cmd, databaseTableName, tableid, chunkSize=1000, testMode=False, updateColumn=None):
    """
        This is a memory-efficient function that reads data in chunks and pushes the chunks as they are read
        so that the full data doesn't need to be held in memory at once.

        :param cmd: String containing the "WHERE" command for use in the query. Should be empty if it's a "full" select
        :param databaseTableName: Name of database table from which data originates
        :param tableid: Numetric tableid of the target warehouse table
        :param chunkSize: default is 1000, but this specifies the size of chunks that are both pulled and pushed
        :param testMode: Simple boolean flag that can be set to true if you just want to try the first 5000 rows.
        :param updateColumn: Column name of the update column in the database table, if needed

        :return: new update value derived from the data that was pulled to do the update
    """
    global logger, jclass, connString, jar

    try:
        conn = jaydebeapi.connect(jclass, connString, jars=jar)
    except jaydebeapi.DatabaseError as de:
        logger.error('Database Connection Error.')
        raise

    # This won't be used often, but you can set testMode to true in the call if you want to just test 5000 rows.
    if testMode == True:
        sql = "SELECT top 5000 * FROM " + databaseTableName + " with (NOLOCK)"
        numRows = 5000
        iterations = math.ceil(5000 / chunkSize)

    # Far more common, setup the process using select * and the Where command
    else:
        sql = "SELECT * FROM " + databaseTableName + cmd + " with (NOLOCK)"
        countSQL = "SELECT COUNT(*) FROM " + databaseTableName + cmd + " with (NOLOCK)"
        numRows = getRowCount(countSQL)
        iterations = math.ceil(numRows / chunkSize)
    logger.info("Executing %s", sql)


    # URL for adding rows to this table
    url = "https://api.numetric.com/v3/table/" + tableid + "/rows"

    # Incorporate the API Key for authorization
    headers = {'authorization': apiKey,
               'Content-Type': "application/json"}

    # Counters to help track progress, and to keep track of the new update value
    rowCounter = 0
    newMax = 0

    try:
        curs = conn.cursor()
        curs.execute(sql)

        columns = [desc[0] for desc in curs.description]  # getting column headers

        for i in range(1, iterations + 1):
            startNum = ((i - 1) * chunkSize)
            endNum = i * chunkSize

            # But also truncate when we go past nrows (happens on last chunk)
            if endNum > numRows + 1:
                endNum = numRows + 1

            try:
                chunk = curs.fetchmany(chunkSize)

                addDf = pd.DataFrame(chunk, columns=columns)
                cleanDF = cleanTable(addDf, databaseTableName)

                # Keep track of new max update value in the data being pulled.
                thisMax = cleanDF.loc[cleanDF[updateColumn].idxmax(), updateColumn]

                # This needs to be compared to the current maximum value because the data is being pulled in (non-ordered) chunks.
                if thisMax > newMax:
                    newMax = thisMax

                # update row counter to help track progress
                rowCounter += len(cleanDF)

                # Convert current chunk to json for sending via the API
                dataToSend = cleanDF.to_json(orient='records')

                # Assembly of the payload for the API call
                payload = "{\"rows\":" + dataToSend + "}"

                # Now initiate the API call

                try:
                    response = requests.request("POST", url, data=payload, headers=headers)
                    # print(response.content)
                    response.raise_for_status()
                # If we catch an HTTPError here, it's like a gateway error that can be fixed by retrying.
                except requests.exceptions.HTTPError as e:
                    # logger.error(e)
                    logger.error("HTTP Error encountered Retrying a few times...")
                    time.sleep(30)
                    for i in range(3):
                        try:
                            response = requests.request("POST", url, data=payload, headers=headers)
                            response.raise_for_status()
                        except requests.exceptions.HTTPError as e:
                            # logger.error(e)
                            logger.error("API still not responding. Trying again.")
                            time.sleep(30)
                except requests.exceptions.RequestException as e:
                    logger.error("Connection Error encountered between row $s and $s.", str(startNum), str(endNum))

                # Update progress.
                logger.info("%s %% of data processed.", str(round(rowCounter / numRows * 100)))

            except jaydebeapi.DatabaseError as de:
                logger.error('Database Connection Error while doing incremental pull. Trying again in 60 seconds')
                logger.error(de)
                raise
        # Having completed all iterations, report that it was successful and send command to process.
        logger.info("Sending command to process new data.")
        sendIndexCommand(tableid)
        return newMax

    except jaydebeapi.DatabaseError as de:
        logger.error('Database Connection Error.')
        raise
    finally:
        curs.close()
        conn.close()


def createTable(df, tableName, primaryKeys, dateColumns, category="API Data"):
    """
    Creates a Numetric warehouse table from a pandas dataframe, returning the tableid
    of the newly create table.

    Uses pandas and requests packages

    :param df: pandas dataframe containing data (and column names)
    :param tableName: The desired Numetric table name
    :param primaryKeys: list of columns comprising the primary key for the table
    :param datColumns: list of columns that should be datetime columns
    :param category: The desired Numetric table category.

    """

    global apiKey, logger

    logger.info('Creating table %s with primary keys %s, dateColumns %s, and category %s', tableName, primaryKeys,
                dateColumns, category)
    # Parse dataframe characteristics for field attributes, which itself is a separate data frame
    fieldAttributes = pd.DataFrame({
        'displayName': list(df),
        'type': df.dtypes})

    # Change the dtypes to Numetric-friendly labels
    fieldAttributes.loc[fieldAttributes.type == 'float64', 'type'] = 'double'
    fieldAttributes.loc[fieldAttributes.type == 'int64', 'type'] = 'integer'
    fieldAttributes.loc[fieldAttributes.type == 'object', 'type'] = 'string'
    fieldAttributes.loc[fieldAttributes.type == 'category', 'type'] = 'string'
    fieldAttributes.loc[fieldAttributes.type == 'bool', 'type'] = 'boolean'

    # Lastly, override some string types to datetime using our list of date columns
    dateIndexes = fieldAttributes.index.isin(dateColumns)
    fieldAttributes.loc[dateIndexes, 'type'] = 'datetime'

    # Convert to JSON format to define fields via the API
    fields = fieldAttributes.to_json(orient='index')

    # Fairly hack-y way to assemble the Primary key string to pass as part of payload.
    # Has to handle multi-column keys if applicable.
    pkString = "["
    for i in range(len(primaryKeys[:-1])):
        pkString += "\"" + primaryKeys[i] + "\","
    pkString += "\"" + primaryKeys[-1] + "\"]"

    # Final assembly of the payload for the API call
    payload = "{\"name\":\"" + tableName + "\"," + "\"primaryKey\":" + pkString + "," + "\"categories\": [\"" + category + "\"]," + "\"fields\":" + fields + "}"
    # Everything is assembled. Now initiate the API call

    # URL for creating a new table
    url = "https://api.numetric.com/v3/table"

    # Incorporate the API Key for authorization
    headers = {'authorization': apiKey,
               'Content-Type': "application/json"}

    # Make the API call, capturing the response code and handling errors
    try:
        response = requests.request("POST", url, data=payload, headers=headers)
        response.raise_for_status()
        logger.info("API response: %s", response.text)
    except requests.exceptions.RequestException as e:
        logger.error(e)
        logger.error('API Connection Error. Could not post new Table to Numetric.')

    # After successful call, return the ID of the newly created table.
    return json.loads(response.text)['id']


def initializeLogger(filename, scriptName) :
    """
        Sets up the logger so that you can call this from multiple scripts generically.

        Uses pandas and requests packages

        :param filename: Desired file name of the log file
        :param scriptName: name of the script to be used in each log entry
    """

    global logger

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(scriptName)

    # create a file handler
    handler = logging.FileHandler(filename)
    handler.setLevel(logging.INFO)

    # create a logging format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(handler)

    logger.info('Script %s started.', scriptName)

    return logger


def getRowCount(sql):
    """
        Gets the row count using the provided SQL. Really this just handles the parsing of the results into a single
        number, since the logic for the SQL has to be customized prior to the function's call

        :param sql: SQL containing a "SELECT count(___) ..." query.

        :return: row count if successful, -1 if there was a problem
    """

    global jclass, connString, jar

    # Set default to -1
    rowCount = -1

    try:
        conn = jaydebeapi.connect(jclass, connString, jars=jar)

        curs = conn.cursor()
        curs.execute(sql)

        data = curs.fetchall()

        # Parse data tuple for the actual row count value
        rowCount = data[0][0]

    except jaydebeapi.DatabaseError as de:
        logger.error('Database Connection Error. Returning -1 for count.')
        logger.error(de)
    finally:
        curs.close()
        conn.close()

    return rowCount.intValue()
