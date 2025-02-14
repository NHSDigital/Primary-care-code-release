import logging
from src.utils.connection_fxns import execute_sql
from sql.sql_testmode_db_setup import test_setup

class class_UKSNOMEDCT_dbs:
    """
    Defines all tables for the UK SNOMEDCT database as properties, as per the config file.
    """
    def __init__(self, SQLserver, dbUKSNOMEDCT:dict, configIA0):
        #defining PROPERTIES of the class

        #assumed schema is dbo
        self.server = SQLserver
        
        #UK SCT release database
        self.prior_db  = dbUKSNOMEDCT['prior_db']
        self.db        = dbUKSNOMEDCT['db']
        self.live      = dbUKSNOMEDCT['db'] # if running in test mode, this will actually be the live db name

        #corresponding tables
        self.SCT_CONCEPT        = f"{self.db}.dbo.{dbUKSNOMEDCT['SCT_CONCEPT']}"
        self.SCT_DESCRIPTION    = f"{self.db}.dbo.{dbUKSNOMEDCT['SCT_DESCRIPTION']}"
        self.SCT_REFSET         = f"{self.db}.dbo.{dbUKSNOMEDCT['SCT_REFSET']}"
        
        #corresponding tables - previous release
        self.prior_SCT_CONCEPT        = f"{self.prior_db}.dbo.{dbUKSNOMEDCT['SCT_CONCEPT']}"
        self.prior_SCT_DESCRIPTION    = f"{self.prior_db}.dbo.{dbUKSNOMEDCT['SCT_DESCRIPTION']}"
        self.prior_SCT_REFSET         = f"{self.prior_db}.dbo.{dbUKSNOMEDCT['SCT_REFSET']}"

        #define IA0 prefix tables - if the full release has already been imported, these will be the same as above
        #this shouldn't ever be the case when using monolith
        if configIA0['IA_SCT_Chosen'] == False:
            # add in SCT concept prefix if release not imported
            self.SCT_CONCEPT_prefix = f"{self.db}.dbo.{configIA0['IA_input']}_{dbUKSNOMEDCT['SCT_CONCEPT']}"
            self.SCT_DESCRIPTION_prefix = f"{self.db}.dbo.{configIA0['IA_input']}_{dbUKSNOMEDCT['SCT_DESCRIPTION']}"
            self.SCT_REFSET_prefix = f"{self.db}.dbo.{configIA0['IA_input']}_{dbUKSNOMEDCT['SCT_REFSET']}"
        else:
            self.SCT_CONCEPT_prefix = self.SCT_CONCEPT
            self.SCT_DESCRIPTION_prefix = self.SCT_DESCRIPTION
            self.SCT_REFSET_prefix = self.SCT_REFSET
        

class class_LocalSNOMEDCT_dbs:
    """
    Defines all tables for your local SNOMEDCT database as properties, as per the config file
    """
    def __init__(self, SQLserver, dbLocSNOMEDCT:dict):
        #defining PROPERTIES of the class

        #assumed schema is dbo
        self.server = SQLserver

        #Local SCT refset release database
        self.db = dbLocSNOMEDCT['db']
        self.live = dbLocSNOMEDCT['db'] # if running in test mode, this will actually be the live db name

        #corresponding tables
        self.Concept            = f"{self.db}.dbo.{dbLocSNOMEDCT['Concept']}"
        self.Refsets            = f"{self.db}.dbo.{dbLocSNOMEDCT['Refsets']}"
        self.ModuleDependency   = f"{self.db}.dbo.{dbLocSNOMEDCT['ModuleDependency']}"
        self.TRUD_ID_Gen        = f"{self.db}.dbo.{dbLocSNOMEDCT['TRUD_ID_Generator']}"
        self.Description        = f"{self.db}.dbo.{dbLocSNOMEDCT['Description']}"
        self.Relationship       = f"{self.db}.dbo.{dbLocSNOMEDCT['Relationship']}"
        self.OwlExpression      = f"{self.db}.dbo.{dbLocSNOMEDCT['OwlExpression']}"
        self.Language           = f"{self.db}.dbo.{dbLocSNOMEDCT['Language']}"
        self.RefsetDescriptor   = f"{self.db}.dbo.{dbLocSNOMEDCT['RefsetDescriptor']}"
        self.PrevRefsetSnapshot = f"{self.db}.dbo.{dbLocSNOMEDCT['PrevRefsetSnapshot']}"


class class_ClusterManagement_dbs:
    """
    Defines all tables for your cluster management database as properties, as per the config file
    """
    def __init__(self, SQLserver, dbCM:dict):
        #defining PROPERTIES of the class

        #assumed schema is dbo
        self.server = SQLserver

        #Cluster management database
        self.db = dbCM['db']
        # corresponding tables
        self.Clusters           = f"{self.db}.dbo.{dbCM['Clusters']}"
        self.Cluster_Ruleset    = f"{self.db}.dbo.{dbCM['Cluster_Ruleset']}"
        self.Cluster_Output     = f"{self.db}.dbo.{dbCM['Cluster_Output']}"
        self.Cluster_Population = f"{self.db}.dbo.{dbCM['Cluster_Population']}"
        self.Rulesets           = f"{self.db}.dbo.{dbCM['Rulesets']}"
        self.Ruleset_Published  = f"{self.db}.dbo.{dbCM['Ruleset_Published']}"
        self.Outputs            = f"{self.db}.dbo.{dbCM['Outputs']}"
        self.Output_Ruleset     = f"{self.db}.dbo.{dbCM['Output_Ruleset']}"
        self.Output_Population  = f"{self.db}.dbo.{dbCM['Output_Population']}"
        self.Populations        = f"{self.db}.dbo.{dbCM['Populations']}"
        self.Population_Ruleset = f"{self.db}.dbo.{dbCM['Population_Ruleset']}"
        self.Code_Decision_Log_Archive = f"{self.db}.dbo.{dbCM['Code_Decision_Log_Archive']}"

#-------------------------------------------------------------

def replace_db_dates_config_params(config, configDates):
    
    #replace MmmYY and previous refset snapshot values in LiveDatabases dictionary to defined MmmYY and last release date (PrevPCDrelDate) values
    for db in config['LiveDatabases']:
        #for all databases with a '.db', replace MmmYY string
        config['LiveDatabases'][db]['db'] = config['LiveDatabases'][db]['db'].replace('MmmYY',configDates.MmmYY)
        if db == 'LocalSNOMEDCT':
            config['LiveDatabases'][db]['PrevRefsetSnapshot'] = config['LiveDatabases'][db]['PrevRefsetSnapshot'].replace('PrevPCDrelDate',configDates.PrevPCDrelDate)

    #replace previous refset snapshot value in TestDatabases dictionary to the defined last release date (PrevPCDrelDate) value
    config['TestDatabases']['LocalSNOMEDCT']['PrevRefsetSnapshot'] = config['TestDatabases']['LocalSNOMEDCT']['PrevRefsetSnapshot'].replace('PrevPCDrelDate',configDates.PrevPCDrelDate)

    return config
#-------------------------------------------------------------

def set_dbs_live_or_test(config,test:bool):
      
    #import variables from the config file    
    SQLserver = config['Connection']['SQL_Server']

    live_db = config['LiveDatabases']
    test_db = config['TestDatabases']

    if test == False:
        #databases are defined by live tables given in config
        dbUKSNOMEDCT = live_db['UKSNOMEDCT']
        dbLocalSNOMEDCT = live_db['LocalSNOMEDCT']
    else:
        #databases are defined by test tables given in config
        dbUKSNOMEDCT = test_db['UKSNOMEDCT']
        dbLocalSNOMEDCT = test_db['LocalSNOMEDCT']
        #test databases are created or updated with contents of live tables, so process can be run as accurately as possible
        replace_test_db(test_db, live_db, SQLserver)
    
    dbClusterManagement = live_db['ClusterManagement']
    
    #create the database classes
    UKSNOMEDCT = class_UKSNOMEDCT_dbs(SQLserver, dbUKSNOMEDCT, configIA0=config['SQL_table_prefix'])
    UKSNOMEDCT.live = live_db['UKSNOMEDCT']['db']
    LocalSNOMEDCT = class_LocalSNOMEDCT_dbs(SQLserver, dbLocalSNOMEDCT)
    LocalSNOMEDCT.live = live_db['LocalSNOMEDCT']['db']
    ClusterManagement = class_ClusterManagement_dbs(SQLserver, dbClusterManagement)

    logging.info(f"Using {UKSNOMEDCT.db} as the UK SNOMED CT database, {LocalSNOMEDCT.db} as the Local SNOMED CT database, and {ClusterManagement.db} as the Cluster Management database.")

    return UKSNOMEDCT, LocalSNOMEDCT, ClusterManagement

def replace_test_db(test_db:dict, live_db:dict, server):
    #loop through databases
    for db in test_db.keys():
        test_db_conn = test_db[db]['db']
        live_db_conn = live_db[db]['db']
        #loop through tables in databases
        for table in live_db[db].keys():
            #ignore if key is db
            if table == 'db' or table == 'prior_db':
                continue
            test_tbl_name = test_db[db][table]
            live_tbl_name = live_db[db][table]
            #define query as SQL code to copy live table into test table and execute
            query = test_setup(test_db_conn, live_db_conn, test_tbl_name, live_tbl_name)
            execute_sql(server, database=live_db_conn, query=query)
            logging.info(f"Created {test_db_conn}.dbo.{test_tbl_name} based on {live_db_conn}.dbo.{live_tbl_name}")