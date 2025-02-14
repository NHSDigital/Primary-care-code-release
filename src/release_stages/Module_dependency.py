import pandas as pd
import logging
from sql.sql_mod_dep import sql_query_monolith_exists, sql_query_pd_actual_md, sql_query_monolith_replace_tar
from sql.sql_checking_queries import sql_query_monolith_TRUD_clin_check
from src.utils.connection_fxns import  get_df_from_sql, execute_sql
from src.utils.dates import class_Dates
from src.utils.databases import class_UKSNOMEDCT_dbs, class_LocalSNOMEDCT_dbs

def mod_dep_dates(configDates:class_Dates, UKSNOMEDCT:class_UKSNOMEDCT_dbs, LocalSNOMEDCT:class_LocalSNOMEDCT_dbs):
    """
    Function Actions:
    - If monolith release exists: finds the international, drug, and pathology release dates from the module dependency monolith snapshot SQL table.
    - If monolith release does not exist: ask user for manual input of dates.
    - Returns a dataframe which can be used in Monolith_replacements() function.
    """   
    logging.info('Finding international, drug and pathology release dates from Module Dependecy table...')
    

    # Check if monolith release exists
    MD_monolith_exists_data = get_df_from_sql(server=UKSNOMEDCT.server,
                                              database=UKSNOMEDCT.db,
                                              query=sql_query_monolith_exists(configDates, 
                                                                              UKSNOMEDCT))

    if MD_monolith_exists_data.iloc[0,0] == 1:
        #Pull out the module IDs we depend on and what their dates are from the monolith module dependency table
        MD_pd_actual_data = get_df_from_sql(server=UKSNOMEDCT.server,
                                            database=UKSNOMEDCT.db,
                                            query=sql_query_pd_actual_md(configDates, 
                                                                         UKSNOMEDCT, 
                                                                         LocalSNOMEDCT))
        
        logging.info("Monolith data created via Query.")  

        #If the monolith module dependency table exists then we're fine, if not, user needs to define UK drugs, international and pathology dates. 
    else:
        #Ask the user for the dates:
        YYYYMMIN = input('International table date in format YYYYMMDD:')
        YYYYMMDR = input('UK drugs table date in format YYYYMMDD:')
        YYYYMMPT = input('Pathology (UTL) table date in format YYYYMMDD:')
        
        #Manually make module dependency rows
        md_data = [
            ['Drug','999000011000001104', YYYYMMDR]
            ,['International','900000000000012004', YYYYMMIN]
            ,['International','900000000000207008', YYYYMMIN]
            ,['UK','999000041000000102', configDates.UKreleaseDate]
            ,['UK other','999000011000000103', configDates.UKreleaseDate]
            ,['UK other','999000021000000109', configDates.UKreleaseDate]
            ,['Pathology','1326031000000103', YYYYMMPT]
            ]
    
        # Create the monolith pandas DataFrame
        MD_pd_actual_data = pd.DataFrame(md_data, columns=['ReleaseType','moduleId','sourceEffectiveTime'])
        logging.info("Monolith data created manually. Dates inputted are as follows: 'Drug':" + YYYYMMDR + ", 'International':" + YYYYMMIN + " and 'Pathology:'"  + YYYYMMPT)

    return MD_pd_actual_data
        
#------------------------------------------------------------------------------------------------------------------------------------------
    
def Monolith_replacements(MD_pd_actual_data, configDates:class_Dates, LocalSNOMEDCT:class_LocalSNOMEDCT_dbs):
    """
    Function Actions:
    - Adds in each of the module dependencies to Local SNOMED CT database Module dependency table, where our release is dependent on these 
    other external releases.
    """ 

    logging.info('Running monolith replacements...')

    #known (static) variables: 
    v_moduleID = '999000011000230102'
    v_refsetID = '900000000000534007'
    #v_UKComposition = '83821000000107'

    #Add in each of the module dependencies where our release is dependent on these other external releases 
    #(i.e. we are the source and are dependent on the target)

    x = 0   #x = a row in the identified module dependencies
    #Loop through table adding in module dependencies
    while x <= (MD_pd_actual_data.shape[0] - 1):
        
        #make sure the row has a date. 
        #If the date is null... 
        if MD_pd_actual_data.isnull().iloc[x, 2] == True:
            
            #...then try calculate it if it is a known 'missing' code...
            if MD_pd_actual_data.iloc[x, 1] == '900000000000012004':
                #pull out other row to use: 
                pd_replace = MD_pd_actual_data.loc[MD_pd_actual_data['moduleId'] == '900000000000207008']  
                if pd_replace.shape[0] >= 2:
                    logging.warning('Module dependency table: More than 1 row returned for module "900000000000207008"...check that the dates returned for both are the same.') 
                #replace the date in our current row
                if pd_replace.isnull().iloc[0, 2] == True:
                    logging.info('Monolith replacements error: 900000000000207008 moduleID also has no date...(no international dates are specified)...') 
                    x_date = input('What is the relevant table date for module ' + MD_pd_actual_data.iloc[x, 1] + '? (Should be the same as module 900000000000207008):') 
                    logging.warning(MD_pd_actual_data.iloc[x, 1] + ' using user input date of ' + x_date)
                else:
                    #replace the content at index x with that detailed at index 0
                    MD_pd_actual_data.iloc[x, 2] = pd_replace.iloc[0, 2]
                #set new date as variable
                x_date = MD_pd_actual_data.iloc[x, 2]  
            
            #... or ask the user for an extra input
            else:
                #ask the user to specify:
                x_date = input(f'What is the relevant table date for module {str(MD_pd_actual_data.iloc[x, 1])}?:')
                logging.warning(f'{str(MD_pd_actual_data.iloc[x, 1])} using user input date of {x_date}')
        
        #but if the date was present then set as a variable to use in the upcoming SQL.                          
        else:                         
            x_date = MD_pd_actual_data.iloc[x, 2]                         
                                
        #set module ID as value variable
        x_value = MD_pd_actual_data.iloc[x, 1]
        
        #Now input a row with the above determined variables into the SQL database...
        execute_sql(server=LocalSNOMEDCT.server,
                    database=LocalSNOMEDCT.db,
                    query=sql_query_monolith_replace_tar(v_moduleID, 
                                                         v_refsetID, 
                                                         x_value, 
                                                         x_date, 
                                                         LocalSNOMEDCT))

        x += 1

    #check table looks as expected? Can remove this part if not valuable 
    MD_TRUD_clin_check_table = get_df_from_sql(server=LocalSNOMEDCT.server,
                                               database=LocalSNOMEDCT.db,
                                               query=sql_query_monolith_TRUD_clin_check(LocalSNOMEDCT,
                                                                                        PCDreleaseDate=configDates.PCDreleaseDate))
    

    ##############################################
    # #if no pathology codes are present, then this won't show up in table. Therefore needs to be manually added here. Can be removed once perminant...function to be further developed
    # pathology_check = (MD_TRUD_clin_check_table['referencedComponentId'].eq('1326031000000103')).any() #does mod_dep overall table contain a row for pathology?
    # if pathology_check == False: 
    #     #add in the pathology row
    #     x_date = input('What is the relevant table date for the Pathology module?: ')
    #     x_value = '1326031000000103'

    #     #Now input a row with the above determined variables into the SQL database...
    #     execute_sql(server=LocalSNOMEDCT.server,
    #                 database=LocalSNOMEDCT.db,
    #                 query=sql_query_monolith_replace_tar(v_moduleID, 
    #                                                      v_refsetID, 
    #                                                      x_value, 
    #                                                      x_date, 
    #                                                      LocalSNOMEDCT) )
    
    # else:
    #     pass

    logging.info("The latest rows from the Module Dependency table (where effectiveTime is the PCD Release Date): ")
    logging.info(MD_TRUD_clin_check_table)
    logging.info('Monolith replacements completed.')