import logging
import datetime
from pathlib import PurePath
from sql.sql_checking_queries import sql_query_Ruleset_Pub_Update_check
from src.utils.connection_fxns import get_df_from_sql
from src.utils.databases import class_ClusterManagement_dbs
from src.utils.file_fxns import csv_save

def update_RP(ClusterManagement:class_ClusterManagement_dbs,PCDreleaseDate,output:PurePath):
    """__Update Ruleset Publication Table__
    - Before updates to the PCD Reference Set Portal take place, it is important to ensure that the Ruleset publication table within SQL is up to date. 
    - This is updated as and when business rules (including ‘Subject to Testing’ versions’) have been published to the business rules webpage.
    - This returns all the published rulesets and their current Issues status.
    """

    #return current Ruleset_published Table
    PBI_Ruleset_Pub_check_df = get_df_from_sql(server=ClusterManagement.server, 
                                            database=ClusterManagement.db, 
                                            query=sql_query_Ruleset_Pub_Update_check(ClusterManagement))
    
    #if any rows containing "Issue" in the 'Publication_issues' column, the script should not run powerbi updates
    if len(PBI_Ruleset_Pub_check_df[PBI_Ruleset_Pub_check_df['Publication_issues'].str.lower().str.contains('issue', na=False)]) > 0:
        logging.error(f'Services which are no longer active have been found in {ClusterManagement.Ruleset_Published}. Please review CHECK_Ruleset_Published_table.csv and contact service leads with any queries. \nIf entries have expired (i.e., the service end date has passed), it is likely they will need retiring/signing off in the {ClusterManagement.Ruleset_Published} table and a later version (if in {ClusterManagement.Rulesets} table) adding to {ClusterManagement.Ruleset_Published}.')
        errortype = 1
    
    #if any rows containing 'still ongoing' in the 'Publication_issues' column, needs checking if current version of business rules have been published.
    elif len(PBI_Ruleset_Pub_check_df[PBI_Ruleset_Pub_check_df['Publication_issues'].str.lower().str.contains('still ongoing', na=False)]) > 0:
        rsinactive_list = PBI_Ruleset_Pub_check_df['Ruleset_Date_Inactive'].tolist()
        errortype = 0 # the following code checks if the ruleset inactive date is in the past and changes the error type accordingly.
        for i in rsinactive_list:                  
            if i is not None:
                inactiveDate = datetime.datetime.strptime(i, '%Y-%m-%d').date()
                if inactiveDate <= datetime.datetime.strptime(PCDreleaseDate, '%Y%m%d').date():
                    logging.debug(f'Ongoing services are present in the {ClusterManagement.Ruleset_Published} table. Please check that the next version has been published as well.')
                    errortype = 2

    #entire table is "Active ruleset"
    elif len(PBI_Ruleset_Pub_check_df[PBI_Ruleset_Pub_check_df['Publication_issues'].str.lower().str.contains('active ruleset', na=False)]) == len(PBI_Ruleset_Pub_check_df):
        logging.info(f'All services in the {ClusterManagement.Ruleset_Published} table are active.')
        errortype = 0

    else:
        logging.info(f'{ClusterManagement.Ruleset_Published} is not appearing as expected. Please thoroughly investigate.')
        errortype = 1

    #save ruleset_published table down in outputs folder for checking
    csv_save(PBI_Ruleset_Pub_check_df, "CHECK_Ruleset_Published_table.csv", output)  

    return errortype 