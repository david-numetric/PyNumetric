import numetricUtil as nu
import jaydebeapi

# Initialize logger
logger = nu.initializeLogger('populate.log','populate')

# Populate tables is usually done in testing mode first. Which limits the first pull to 5000 records.
stillTesting = True

try:
    for index, row in nu.tables.iterrows():
        # First get the details for the current table from the master list of tables
        tableid = nu.getTableId(row['numetricTableName'])
        databaseTableName = row['databaseTableName']

        if tableid == 'NA':
            nu.logger.info("There was a problem. Table %s didn't return an id.", databaseTableName)
        else:
            # Now use the updateRows function to update the table
            nu.readAndPush("", databaseTableName, tableid, chunkSize=400)

except jaydebeapi.DatabaseError as de:
    logger.error("The process failed because of a database error.")
    logger.exception(de)
except Exception as e:
    logger.error("The populate process failed.")
    logger.exception(e)
else:
    logger.info("All data updated successfully. Updating \"LastRun\" to now.")
