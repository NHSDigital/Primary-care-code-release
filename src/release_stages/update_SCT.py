import logging
import sys
from src.utils.dates import class_Dates
from src.utils.databases import class_UKSNOMEDCT_dbs, class_LocalSNOMEDCT_dbs
from src.utils.connection_fxns import get_df_from_sql, execute_sql
from sql.sql_checking_queries import sql_QA_query_PSU_counts, sql_QA_query_TRUD_counts
from sql.sql_PostPub_PSU import sql_query_PostPub_PSU_db_updates


def update_SCT_tables(configDates:class_Dates, test:bool, UKSNOMEDCT:class_UKSNOMEDCT_dbs, LocalSNOMEDCT:class_LocalSNOMEDCT_dbs):
    """
    Function Actions:
    - Updates SCT tables in live database with new snpashot data (completed if not test mode).
    - Perform checks to make sure that it updated correctly.
    """

    logging.info('Updating SCT tables...')
    
    #Run the PSU updates SQL code
    execute_sql(server=UKSNOMEDCT.server,
                database=UKSNOMEDCT.db,
                query=sql_query_PostPub_PSU_db_updates(UKSNOMEDCT, 
                                                            LocalSNOMEDCT, 
                                                            configDates))
    logging.info(f'SCT tables updated in {UKSNOMEDCT.db} database.')

    #check to see that the UK SNOMED CT db has been updated correctly
    PSU_counts_data = get_df_from_sql(server=UKSNOMEDCT.server,
                                      database=UKSNOMEDCT.db,
                                      query=sql_QA_query_PSU_counts(UKSNOMEDCT))

    TRUD_counts_data = get_df_from_sql(server=LocalSNOMEDCT.server,
                                       database=LocalSNOMEDCT.db,
                                       query=sql_QA_query_TRUD_counts(LocalSNOMEDCT, configDates))

    logging.info(' PSU_Counts: ')
    logging.info(           PSU_counts_data)
    logging.info(' TRUD_Counts: ')
    logging.info(           TRUD_counts_data)

    #produce error message if PSU and TRUD counts are not the same.
    if PSU_counts_data.equals(TRUD_counts_data) != True and test == False:
        logging.error('Stage 6 has errored out during completion. Please query or complete manually.')
        logging.critical(F'{LocalSNOMEDCT.db,} counts are not equal to {UKSNOMEDCT.db} counts. Script has been forced exit, please investigate.')
        sys.exit(0)
    else: 
        logging.info('Stage 6 completed successfully.')