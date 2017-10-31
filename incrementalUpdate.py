import numetricUtil as nu
import jaydebeapi
import json
import scratch3
from datetime import datetime, timedelta


# Initiate logger
logger = nu.initializeLogger('incremental.log','incremental udpate')


try:
    for index, row in nu.tables.iterrows():
        # First get the details for the current table from the master list of tables (which comes from the config file
        tableid = nu.getTableId(row['numetricTableName'])
        databaseTableName = row['databaseTableName']
        updateColumn = row['updateColumn']
        updateValue = row['lastUpdateValue']

        # Shouldn't ever get NA back from getTableID because this is an incremental update...might as well check, though
        if tableid == 'NA':
            nu.logger.info("There was a problem. Table %s didn't return an id.", databaseTableName)
        else:
            # Now use the updateRows function to update the table
            newUpdateValue = nu.updateRows(tableid, databaseTableName, updateColumn, updateValue, chunkSize=400)
            # And write the new update file to the config (still in memory) so it can be sent back out to the config file
            row['lastUpdateValue'] = str(newUpdateValue)
except jaydebeapi.DatabaseError as de:
    logger.error("The incremental update failed because of a database error. Not going to update config")
    logger.exception(de)
except Exception as e:
    logger.error("The incremental update failed. Not going to update config.")
    logger.exception(e)
else:
    # If no errors occurred, the incremental update was successful, and we can write the updated info back out to disk.
    logger.info("All data updated successfully. Updating \"LastRun\" to now, updating config file.")
    nu.configData['lastRun'] = datetime.now().__format__('%Y-%m-%d %H:%M:%S')
    nu.configData['tables'] = nu.tables.to_dict('records')

    # And send the updated info back out to the config.json file.
    with open('config.json', 'w') as outfile:
        json.dump(nu.configData, outfile)
