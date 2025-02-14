#stage 7 functions
import os
import shutil
import pandas as pd
import logging
import sys
from pathlib import PurePath
from unidecode import unidecode
from email.mime.text import MIMEText
from src.utils.dates import class_Dates
from src.utils.databases import class_UKSNOMEDCT_dbs, class_LocalSNOMEDCT_dbs, class_ClusterManagement_dbs
from src.utils.connection_fxns import get_df_from_sql
from src.utils.file_fxns import csv_save, excel_save, zip_files, email, bulk_replace_str_xlsx
from src.utils.formatting_fxns import substr_in_col_bool, bool_condform, false_bold_condform, identify_empty_cols
from sql.sql_Reference_data_creation import sql_query_A_Ref_Data_Creation, sql_query_B_Ref_Data_Creation
from sql.sql_simple_queries import sql_query_select_all, sql_query_select_distinct
from sql.sql_checking_queries import sql_QA_query_GPData_count
from sql.sql_firearms import sql_query_Firearms_ref_data

#---------------------------------------------------------------------------------------------

def supportingProducts(configDates:class_Dates, filepath_dict:dict, email_dict:dict, config:dict, UKSNOMEDCT:class_UKSNOMEDCT_dbs, LocalSNOMEDCT:class_LocalSNOMEDCT_dbs, ClusterManagement:class_ClusterManagement_dbs, Name:str, test:bool):
    """
    Function Actions:
    - Creates three GDPPR and GPData tables in our UK SNOMED CT database.
    - Perform checks these were created correctly.
    - Saves two of these as CSVs.
    - Produces a spreadsheet containing firearms active/inactive content.
    - Creates a zip file of the csvs
    - Creates an email with the zip file attached.
    """
    logging.info('Supporting Products being generated...')

    publishing_TRUD_fldr = filepath_dict['publishing_TRUD_fldr']
    SupportingProducts_fldr = filepath_dict['SupportingProducts_fldr']

    #Create the reference data SQL tables
    SQL_RefData_B_data = referenceDataCreate(configDates, UKSNOMEDCT, ClusterManagement)

    #Perform QA checks
    referenceDataQA(configDates=configDates, nonascii_accpt_diff=config['Checking_lists']['accepted_diff_non_ascii'], publishing_TRUD_fldr=publishing_TRUD_fldr, SQL_RefData_B_data=SQL_RefData_B_data, test=test, UKSNOMEDCT=UKSNOMEDCT, ClusterManagement=ClusterManagement)
    
    #Export SQL tables as GDPPR and GPData csv files and returns filenames
    GDPPR_GPdata_fl = GDPPR_GPData(configDates=configDates, SupportingProducts_fldr=SupportingProducts_fldr, UKSNOMEDCT=UKSNOMEDCT)
    
    #Run the Firearms.sql query + create excel spreadsheet
    firearms(configDates, publishing_TRUD_fldr, SupportingProducts_fldr, UKSNOMEDCT=UKSNOMEDCT, LocalSNOMEDCT=LocalSNOMEDCT)

    #Create zipfile of GDPPR and GPData csvs
    csvs_loc = [{'filepath':SupportingProducts_fldr, 
                'name':'GPData_' + GDPPR_GPdata_fl},
                {'filepath':SupportingProducts_fldr, 
                'name':'GDPPR_' + GDPPR_GPdata_fl}]
    zip_loc = {'filepath':publishing_TRUD_fldr, 
               'name':configDates.YYYYMMDD(configDates.PCDreleaseDate) +'_GPdata_and_GDPPR_files.zip'}
    
    zip_files(content_filepath_name=csvs_loc, zf_filepath_name=zip_loc)

    #Email body and specific files detailed for email creation
    body = email_dict['body']
    body = body.replace("<GDPPR_GPdata_fl>", GDPPR_GPdata_fl)
    body = MIMEText(body, 'plain', 'utf-8')

    files = [PurePath(zip_loc['filepath'],zip_loc['name'])]
    
    #final composition of email
    email(email_subject=email_dict['subject'], 
          email_To=email_dict['To'], 
          email_Cc=email_dict['Cc'], 
          email_body=body, 
          outfile_path=publishing_TRUD_fldr, 
          outfile_name="PCD_release_Support_docs.eml", 
          attach=True, 
          attachment_files=files)

#---------------------------------------------------------------------------------------------
#---------------------------------------------------------------------------------------------

def referenceDataCreate(configDates:class_Dates, UKSNOMEDCT:class_UKSNOMEDCT_dbs, ClusterManagement:class_ClusterManagement_dbs) -> pd.DataFrame:
    """
    Function Actions:
    - Creates the GDPPR and GPData (reference) tables (when test = false).
    """
    
    # create the first table (GPData Cluster refset) using the SCT_REFSET, SCT_DESCRIPTION and SCT_CONCEPT tables
    # deduplicate
    # replace any special characters so that the file is exported with the correct encoding
    # return the number of clusters and rows in this table
    SQL_RefData_A_data = get_df_from_sql(server=UKSNOMEDCT.server,
                                            database=UKSNOMEDCT.db,
                                            query=sql_query_A_Ref_Data_Creation(configDates, 
                                                                                UKSNOMEDCT, 
                                                                                ClusterManagement))

    # check the table just created has no special characters and no null rows
    # create the second and third tables from the first (GPData cluster refset nc and GDPPR Cluster refset)
    SQL_RefData_B_data = get_df_from_sql(server=UKSNOMEDCT.server,
                                            database=UKSNOMEDCT.db,
                                            query=sql_query_B_Ref_Data_Creation(configDates,
                                                                                UKSNOMEDCT,
                                                                                ClusterManagement))
    
    logging.info(f"GPData cluster refset counts: Number of clusters is {SQL_RefData_A_data.iloc[0]['Number of clusters']}, number of rows is {SQL_RefData_A_data.iloc[0]['Number of rows']}.")
    logging.info(f'GDPPR and GPData (reference) tables created in {UKSNOMEDCT.db} database.')

    return SQL_RefData_B_data

#---------------------------------------------------------------------------------------------

def referenceDataQA(configDates:class_Dates, nonascii_accpt_diff, publishing_TRUD_fldr:str, SQL_RefData_B_data:pd.DataFrame, test:bool, UKSNOMEDCT:class_UKSNOMEDCT_dbs, ClusterManagement:class_ClusterManagement_dbs):
    """
    QA checks on Reference data tables:
    1. Check that the SQL_RefData_B_data table is empty.
    2. Check GPData and GDPPR tables for non-ascii characters.
    3. Check against previous release's tables to see if the numbers have increased (expectation: to increase or remain the same).
    """
    
    #-----------------------------------------------------------------------------------------------------
    #1.
    #check there are no rows with a null concept description
    #check there are no more special characters
    #check there NULL cluster categories
    #if checks are okay - table is blank, skipss the rests of this section
    if SQL_RefData_B_data.empty == True:
        logging.info('Reference tables QA shows table to be empty, as expected.')
        
    #table should be blank, if not force stop cell and investigate errors in csv
    else:
        csv_save(df=SQL_RefData_B_data, filename='CHECK_reference_data_creation.csv', filepath=publishing_TRUD_fldr)
        logging.error('Reference tables check 1 has failed. There looks to be some errors, please investigate the problems and re-run.')
        # code will only error out if not running in test mode, since the new reference tables are being used.
        if test == False:
            logging.critical('Null rows or special characters have been found in the GDPPR and GPData reference tables. Script has been forced exit, please investigate using CHECK_reference_data_creation.csv.')
            sys.exit(0)

    #-----------------------------------------------------------------------------------------------------
    #2.
    #Check the GPData and GDPPR tables for any unknown non-ascii character combinations
    logging.info('Checking GPData and GDPPR tables for any unknown non-ascii character combinations')
    #return GPData and GDPPR tables from SQL
    # maybe due to python exporting differently to SQL, (SQL doesn't recognise the commas as delimitators), then do we need to continue to create "_nc" copies of tables?
    
    GPData_table_data = get_df_from_sql(server=UKSNOMEDCT.server,
                                        database=UKSNOMEDCT.db,
                                        query=sql_query_select_all(database=UKSNOMEDCT.db, #in test, this table should have just been created
                                                                   table_name=f'GPData_Cluster_refset_1000230_{configDates.YYYYMMDD(configDates.PCDreleaseDate)}_nc'))

    GDPRR_table_data = get_df_from_sql(server=UKSNOMEDCT.server,
                                       database=UKSNOMEDCT.db,
                                       query=sql_query_select_all(database=UKSNOMEDCT.db, #in test, this table should have just been created
                                                                  table_name=f'GDPPR_Cluster_refset_1000230_{configDates.YYYYMMDD(configDates.PCDreleaseDate)}_nc'))

    #check concept description and refset description columns in tables for any non-ascii characters
    ascii_checking1 = ascii_checking(GDPRR_table_data, 'ConceptId_Description', error_Loc='GDPRR ConceptId_Description')
    ascii_checking2 = ascii_checking(GDPRR_table_data, 'Refset_Description', error_Loc='GDPRR Refset_Description')
    ascii_checking3 = ascii_checking(GPData_table_data, 'ConceptId_Description', error_Loc='GPData ConceptId_Description')
    ascii_checking4 = ascii_checking(GPData_table_data, 'Refset_Description', error_Loc='GPData Refset_Description')
    #merge all tables 
    df_nonascii = pd.concat([ascii_checking1, ascii_checking2, ascii_checking3, ascii_checking4], ignore_index=True)

    if df_nonascii.empty == False:       #df is not empty and has indentified non-ascii characters
        logging.warning(f'Non-ascii characters found in reference data tables.')

        # conditional_formating check
        # create new col for True/False if non_ascii character is accepted difference character (specified in config)
        df_nonascii['Accepted_Character'] = df_nonascii['Description_with_Error'].str.contains('|'.join(nonascii_accpt_diff))
        
        true_list, false_list = substr_in_col_bool(df_nonascii, 'Accepted_Character', 'Description_with_Error')
        #order should go FALSE > TRUE
        df_nonascii = df_nonascii.sort_values('Accepted_Character')

        #print off the checks only if there is a non-acceptable non-ascii character identified
        if ('FALSE' in set(df_nonascii['Accepted_Character'])) == True:  
                     
            # apply styling and save down edited dataframe
            df_nonascii = df_nonascii.style.applymap(bool_condform).applymap(false_bold_condform)

            #convert df and save as an excel file with instructions
            with pd.ExcelWriter(PurePath(publishing_TRUD_fldr, "CHECK_GDPPR_GPData_potential_chars_errors.xlsx")) as excel_writer:
                about = pd.DataFrame({' Instructions':["This file flags up any non-ascii characters in the GDPPR and GPData reference tables.",
                                        "If the Accepted_Character is TRUE, these are characters we are aware of but do not cause problems with exporting the reference data tables. No actions are required and this file is for information only.",
                                        "If the Accepted_Character is FALSE, the description contains a character which is new and may cause issues:",
                                        "\u2022 If the code errored out, this character will need to be added into sql query 10, sql_query_A_Ref_Data_Creation as a 'Replace', or investigations into the bulk import need to occur.",
                                        "\u2022 If the code did not error out, open in excel (import the data or it will corrupt!) the appropriate reference data csv (Error_Location col), check that the description is showing without corrupted chars.",
                                        "   \u2218 If the chars are corrupted, amend the bulk import query or add to sql query 10 as find and replace, otherwise if all fine, this can be added to the accepted_diff_non_ascii list in the config file."]})
                about.to_excel(excel_writer, sheet_name='ascii chars', startrow=0, index=False)
                df_nonascii.to_excel(excel_writer, sheet_name='ascii chars', startrow=8, index=False)
            logging.warning(f"CHECK_GDPPR_GPData_potential_chars_errors.xlsx has been saved to {publishing_TRUD_fldr}. Please open and follow instructions at the top of the file.")

        else: 
            logging.info('No unacceptable non-ascii characters have been identified in the GDPPR_GPData combined check table. No further checks are required.')    

    #---------------------------------------------------------------------------------------------
    #3.
    #Check of the previous release's table to see the difference between cluster count. Positive increase expected.
    logging.info("Checking previous release's table to make sure numbers have remained the same or increased.")
    
    last_release = UKSNOMEDCT.live
    last_release = last_release.replace(configDates.MmmYY, configDates.MmmYY_prev)
    #execute sql queries
    SQL_prev_yr_data = get_df_from_sql(server=UKSNOMEDCT.server, 
                                       database=last_release,
                                       query=sql_QA_query_GPData_count(db=last_release,
                                                                       PCDreleaseDate=configDates.YYYYMMDD(configDates.PrevPCDrelDate)))

    SQL_curr_yr_data = get_df_from_sql(server=UKSNOMEDCT.server,
                                       database=UKSNOMEDCT.db,
                                       query=sql_QA_query_GPData_count(db=UKSNOMEDCT.db,
                                                                       PCDreleaseDate=configDates.YYYYMMDD(configDates.PCDreleaseDate)))

    #calculate the difference between this release and previous release cluster numbers to check this release is higher than the previous

    #join tables togther and calculate difference of clusters
    cluster_count =pd.concat([SQL_prev_yr_data, SQL_curr_yr_data],axis=0) #top/bottom
    cluster_count.insert(loc=0, column='Code release', value = ['Previous', 'Current'])
    cluster_count['Clusters Difference'] = cluster_count['Number of clusters'].diff() 
    cluster_count = cluster_count.reset_index(drop=True)

    #if difference is negative, warning message, this will need investigating manually. If positive, continue. 
    if cluster_count['Clusters Difference'][1] >= 0:
        logging.info('Positive increase in clusters from previous release, as expected.')
    else:
        logging.error("There is no/a negative difference in clusters between this release and last release. This is unusual. Please check that is as expected!!!")
        #Dev note: code previously written for this but needed work - see archive prior to 03/01/2024 under Curr_Prev_Rel_Code_difference function.
#---------------------------------------------------------------------------------------------

def GDPPR_GPData(configDates:class_Dates, SupportingProducts_fldr:str, UKSNOMEDCT:class_UKSNOMEDCT_dbs):
    """
    Function Actions:
    - Saves the GDPPR and GPData tables as csv's to the supporting products folder.
    """
    logging.info("GDPPR and GPData exporting...")

    #Export GPData SQL table and set 2 columns as strings
    GPData_csv_data = get_df_from_sql(server=UKSNOMEDCT.server,
                                      database=UKSNOMEDCT.db,
                                      query=sql_query_select_distinct(database=UKSNOMEDCT.db,
                                                                      table_name=f'GPData_Cluster_refset_1000230_{configDates.YYYYMMDD(configDates.PCDreleaseDate)}_nc'))
    GPData_csv_data['RefsetId'] = GPData_csv_data['RefsetId'].astype('string')
    GPData_csv_data['ConceptId'] = GPData_csv_data['ConceptId'].astype('string')

    #Export GDPPR SQL table and set 2 columns as strings
    GDPPR_csv_data = get_df_from_sql(server=UKSNOMEDCT.server,
                                     database=UKSNOMEDCT.db,
                                     query=sql_query_select_distinct(database=UKSNOMEDCT.db, 
                                                                     table_name=f'GDPPR_Cluster_refset_1000230_{configDates.YYYYMMDD(configDates.PCDreleaseDate)}_nc'))
    GDPPR_csv_data['RefsetId'] = GDPPR_csv_data['RefsetId'].astype('string')
    GDPPR_csv_data['ConceptId'] = GDPPR_csv_data['ConceptId'].astype('string')
    
    #define csv names and save df down as csvs
    GDPPR_GPdata_fl = f'Cluster_Refset_1000230_{configDates.YYYYMMDD(configDates.PCDreleaseDate)}.csv'

    #checks that all cells have content in them (we wouldn't expect any empty rows - helps to identify if there are any large missing parts too)
    identify_empty_cols(GPData_csv_data)
    identify_empty_cols(GDPPR_csv_data)

    #export the GPdata and GDPPR dataframe to csv's

    #update dataframe with de-unicoded string for words / characters that have caused past issues
    #check the saved csv, locate the final row able to print, then locate the later rows back in the python df to view the character causing grief
    GPData_csv_data=GPData_csv_data.replace({'Māori': unidecode('Māori')}, regex=True)
    csv_save(df=GPData_csv_data, filename=f'GPData_{GDPPR_GPdata_fl}', filepath=SupportingProducts_fldr)
    
    #check the saved csv, locate the final row able to print, then locate the later rows back in the python df to view the character causing grief
    GDPPR_csv_data=GDPPR_csv_data.replace({'Māori': unidecode('Māori')}, regex=True)
    csv_save(df=GDPPR_csv_data, filename=f'GDPPR_{GDPPR_GPdata_fl}', filepath=SupportingProducts_fldr)

    return GDPPR_GPdata_fl
#---------------------------------------------------------------------------------------------

def firearms(configDates:class_Dates, publishing_TRUD_fldr, SupportingProducts_fldr, UKSNOMEDCT:class_UKSNOMEDCT_dbs, LocalSNOMEDCT:class_LocalSNOMEDCT_dbs):
    """
    Function Actions:
    - Creates the firearms table in UK SNOMED CT database.
    - Saves the firearms SQL table contents into a template excel spreadsheet in Supporting Products folder.
    - Template already includes conditional formatting.
    - Contents will be ordered by currentMembership desc, conceptStatus, fullySpecifiedName.
    """
    logging.info("Firearms exporting...")

    #Run the Firearms.sql query + append to excel template
    firearms_query = sql_query_Firearms_ref_data(configDates, UKSNOMEDCT, LocalSNOMEDCT)

    firearms_table = get_df_from_sql(server=UKSNOMEDCT.server,
                                     database=UKSNOMEDCT.db,
                                     query=firearms_query)
    
    # Remove the header row >> stops an additional haeder being included in the template later on. Alternative to state skiprows[0] in the read_func
    firearms_table = firearms_table.iloc[1:]
    # Reset the index of the dataframe
    firearms_table = firearms_table.reset_index(drop=True)

    #append the firearms df to a ready made template, found in the templates folder, and save as a new excel spreadsheet
    
    # create name of firearms file with correct YYYYMMDD
    firearms_file = PurePath(publishing_TRUD_fldr, f'Firearms_trigger_refset_membership_{configDates.YYYYMMDD(configDates.PCDreleaseDate)}.xlsx') 
    
    # if firearms file already exists in publication folder, delete and rename copied template
    if os.path.exists(firearms_file):
        os.remove(firearms_file)
    os.rename(PurePath(publishing_TRUD_fldr, 'Firearms_trigger_refset_membership_YYYYMMDD.xlsx'), firearms_file)

    # append firearms data to firearms file. This will overwrite
    with pd.ExcelWriter(firearms_file, mode='a', engine='openpyxl', if_sheet_exists='overlay') as writer:  #to use 'overlay' need pandas version 1.4.0+
        firearms_table.to_excel(writer, sheet_name='999031731000230105', startrow=2, header=False, index=False)

    #update title in xlsx file with the release date:  "Firearms trigger codes simple reference set (DDMMYYY)"
    bulk_replace_str_xlsx(publishing_TRUD_fldr, f'Firearms_trigger_refset_membership_{configDates.YYYYMMDD(configDates.PCDreleaseDate)}.xlsx', "999031731000230105", '[Release_Date]', str('Release Date: ' + str(configDates.YYYYMMDD(configDates.PCDreleaseDate))))

    # copies firearms file into SupportingProducts folder inside SNOMEDCT_... folder
    try:
        shutil.copyfile(firearms_file, PurePath(SupportingProducts_fldr, f"Firearms_trigger_refset_membership_{configDates.YYYYMMDD(configDates.PCDreleaseDate)}.xlsx"))
    except shutil.SameFileError:
        logging.error('Firearms not copied to SupportingProducts folder since it already exists. Please check this!!')

    logging.info('Firearms SQL table has been created in xlsx with conditional formatting.')

#---------------------------------------------------------------------------------------------

def ascii_checking(df:pd.DataFrame, col_name:str, error_Loc:str) -> pd.DataFrame:
    """
    Function Actions:
    - Checks the contents of a specific column in a dataframe for non-ascii characters.
    - Any non-ascii characters identified are updated and added to the 'Description with Error' column.
    
    Background: "Since ascii characters can be encoded using only 1 byte, so any ascii characters length will be true to its size after encoded to bytes;
    whereas other non-ascii characters will be encoded to 2 bytes or 3 bytes accordingly which will increase their sizes.
    """

    df2 = []
    #turn any none values to empty string (will cause TypeError otherwise)
    df.fillna("",inplace=True)
    isascii = lambda s: len(s) == len(s.encode()) #we can encode the string as UTF-8, then check whether the length stays the same. If so, then the original string is ASCII
    for x in df[col_name]:
        if x == "":
            logging.warning(f'Error - An empty string was found in ascii checking for {col_name}.')
        else:
            if not isascii(x):
                df2.append(x)
    df2 = pd.DataFrame(df2, columns=['Description_with_Error'])
    df2 = df2.assign(Table_Col_Location=error_Loc) #'Table_Col_Location' == column

    return df2

#---------------------------------------------------------------------------------------------