import numetricUtil as nu

# Initialize logger
logger = nu.initializeLogger('newTable.log','newTable')

# Establish table category, which will be applied across all tables. Can be changed later in Numetric if desired.
tableCategory = 'Progress DB'

# Now cycle throug the list of tables (from the config file) and create each table on Numetric
for index, row in nu.tables.iterrows():

    # First get the details for the current table from the master list of tables
    nameOfNewTableInDatabase = row['databaseTableName']
    tableName = row['numetricTableName']
    updateColumn = row['updateColumn']
    primaryKeys = row['primaryKeys']
    dateColumns = row['dateColumns']

    # Build initial query so we can assemble structure and create the table in Numetric
    sql = "SELECT TOP 1000 * FROM " + nameOfNewTableInDatabase

    # execute with SQL. Function returns a pandas dataframe.
    logger.info("Executing %s", sql)
    initialTable = nu.read_jdbc(sql)

    logger.info("Creating Numetric Table %s from database table %s.", tableName, nameOfNewTableInDatabase)
    # Display warning if resulting dataframe is empty
    if len(initialTable.index) == 0:
        logger.info("WARNING: Database table \"%s\" is empty.", nameOfNewTableInDatabase)

    # Get table id if it exists
    tableid = nu.getTableId(tableName)

    # If NA was returned, we need to create the table. And it probably will be NA since this is a newTable script...
    if tableid == 'NA':
        logger.info("Creating table %s on the Numetric platform...", tableName)
        tableid = nu.createTable(initialTable, tableName, primaryKeys, dateColumns, category=tableCategory)
        logger.info("Table \"%s\" successfully created with ID: %s", tableName, tableid)
