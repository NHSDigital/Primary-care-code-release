import logging
import pandas as pd
from src.utils.databases import class_LocalSNOMEDCT_dbs
from sql.sql_TRUD_table_Clearout import sql_query_TRUD_Table_Clearout
from sql.sql_checking_queries import sql_query_TRUD_table_check
from src.utils.connection_fxns import get_df_from_sql, execute_sql

def cleardown_TRUD_tables(LocalSNOMEDCT:class_LocalSNOMEDCT_dbs):
    """
    Function Actions:
    - Identifies all of the current Local SNOMED CT database TRUD tables.
    - Removes historical versions of tables in the Local SNOMED CT database so that only 1 version (the latest) is remaining.
    """
    #listing Local SNOMED CT database tables for removal and confirming choices
    logging.info(f'Running the previous publication {LocalSNOMEDCT.db} Cleardown...')

    #return all latest TRUD tables
    TRUD_Pub_Tab_check = get_df_from_sql(server=LocalSNOMEDCT.server,
                                         database=LocalSNOMEDCT.db,
                                         query=sql_query_TRUD_table_check())
    
    #if none present, then set variable as an empty df
    if TRUD_Pub_Tab_check.empty == True:
        TRUD_Pub_Tab_check = pd.DataFrame({'table_name':[]})

    #add all tables returned into a list
    TRUD_Pub_Tab_check = TRUD_Pub_Tab_check['table_name'].tolist()

    #create a list, but of the returned tables final 8 characters (expected to be dates in YYYYMMDD format)
    TRUD_Pub_Tab_list = [x[-8:] for x in TRUD_Pub_Tab_check]

    set_res = set(TRUD_Pub_Tab_list) #removes any duplicates
    OldRelDates = (list(set_res))
    OldRelDates.sort() # places dates in ascending order

    Old_Rel_Dat_rem = len(OldRelDates) - 1 ##the set number of runs needed to delete tables but leave the latest
    logging.info(f'The snapshot tables ending in the following dates have been identified to be removed from {LocalSNOMEDCT.db}: {(OldRelDates[0:Old_Rel_Dat_rem])}.')

    #force close script if not happy to drop tables with the above dates. Re-run cell once happy again / reviewed. Only happens if not in test mode
    answer = input(f"Please confirm that you're happy for the above date(s) table(s) in the {LocalSNOMEDCT.db} database to be dropped?: [y/n] ")

    #runner is happy for the listed tables to be removed
    if answer.lower().startswith("y") == True:
        logging.info(f'User has accepted to delete the above tables from the {LocalSNOMEDCT.db} database.')

        #active removal of TRUD tables from SQL
        if len(OldRelDates) >=2:
            x = 0
            while x < Old_Rel_Dat_rem: # while loop starts at x = 0
                OldestRelDate = OldRelDates[0]
                #execute the 'remove TRUD table' SQL query
                execute_sql(server=LocalSNOMEDCT.server,
                            database=LocalSNOMEDCT.db,
                            query=sql_query_TRUD_Table_Clearout(LocalSNOMEDCT, OldestRelDate))
                OldRelDates.pop(0) # removes 1st index in list (which is in ascending date order)
                x += 1
            
            logging.info(f'{x} set(s) of tables have been dropped.')
        
        else:
            logging.info('There is currently only 1 or fewer versions of tables available, therefore there have been no changes.')
            
    elif answer.lower().startswith("n") == True:
        logging.info('User has declined to drop the tables from the database. This process has been skipped.')
    
    else:
        logging.error('Incorrect answer format. If you would like the tables to be dropped, you will need to rerun the cleardown_TRUD_tables() function.')
    
    logging.info(f'Previous publication {LocalSNOMEDCT.db} Cleardown complete.')