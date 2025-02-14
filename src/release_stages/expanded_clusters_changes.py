import os
import logging
import openpyxl
import pandas as pd
import numpy as np
import pathlib
import zipfile
import shutil
import math
from pathlib import PurePath
from pathlib import Path
from collections import OrderedDict
from pandas.errors import EmptyDataError
from email.mime.text import MIMEText
from operator import *
import math
from decimal import Decimal
from datetime import date


from src.utils.dates import class_Dates
from src.utils.databases import class_UKSNOMEDCT_dbs, class_LocalSNOMEDCT_dbs, class_ClusterManagement_dbs
from src.utils.file_fxns import email, zip_mult_fldr_files, write_summary_txt_file, bulk_replace_str_xlsx
from src.utils.formatting_fxns import slice_dict_list
from src.utils.connection_fxns import get_df_from_sql
from sql.sql_expanded_cluster_contents import sql_query_static_ruleset_ECC_tbl, sql_query_static_service_ECC_tbl, all_live_upcoming_rulesets_with_clusters, sql_query_changed_with_outcomes_ruleset_ECC_tbl, sql_query_all_cluster_changes, sql_query_changed_with_outcomes_service_ECC_tbl, sql_query_changed_with_outcomes_ruleset_ECC_tbl
from sql.sql_simple_queries import sql_query_select_all_short, sql_query_select_all

#-------------------------------------------------------------------------------------------------
def string_in_df(df, column, accepted_list):
    """ Identifies whether a specified value appear in a df. Returns a boolean answer.  """
    
    filtered_df = df[df[column].isin(accepted_list)]
    if filtered_df.empty == True:
        return False
    else:
        return True    

#-------------------------------------------------------------------------------------------------
def ECC_zip(zipFileName, ECC_ruleset_fldr, ECC_service_fldr, filepath_dict:dict, configDates: class_Dates):
   
    """
    Actions: 
    - Zip together all files within a given ECC outputs folder.
    """

    #zip up the Live ECC changes outputs
        ##Zipping setup
        #set up zipping variables
    ECC_folders = [ECC_ruleset_fldr, ECC_service_fldr]
    ECC_zip_filename = pathlib.Path(filepath_dict['publishing_TRUD_fldr'] , f'{zipFileName}_{configDates.YYYYMMDD(configDates.PCDreleaseDate)}.zip')

    #zip up all files in the ruleset and service directories
    zip_mult_fldr_files(ECC_folders, ECC_zip_filename)

    #print out all of the files that feature in the zipped file
    logging.info(f'Files saved and zipped to {ECC_zip_filename} file...')
    with zipfile.ZipFile(ECC_zip_filename, mode="r") as zip_file:
        logging.info(zip_file.printdir())


def ECC_zip_and_email(configEmails:dict, filepath_dict:dict, configDates: class_Dates):
    """
    Actions: 
    - Zip together all files in the given Live changes, upcoming changes and static ECC folders.
    - Produce an email to send changed ECC zips to DGAT team, along with the ECC_changes_summary_file.txt

    Outputs:
    - 3 zip files
    - 1 email, addressed to DGAT team
        
    """
    
    logging.info('ECC File zipping started...')

    #run file zipping
    #Create zipfile of changed Live ECC_ruleset and ECC_service xlsx files
    ECC_ruleset_fldr = pathlib.Path(filepath_dict['publishing_TRUD_fldr'],'Changed_Expanded_cluster_lists_Ruleset-level', 'Live_Rulesets')
    ECC_service_fldr = pathlib.Path(filepath_dict['publishing_TRUD_fldr'],'Changed_Expanded_cluster_lists_Service-level', 'Live_Services')
    ECC_zip('ECC_changes_LIVE_rulesets_and_services',ECC_ruleset_fldr, ECC_service_fldr, filepath_dict, configDates)

    #Create zipfile of changed Upcoming ECC_ruleset and ECC_service xlsx files
    ECC_ruleset_fldr = pathlib.Path(filepath_dict['publishing_TRUD_fldr'],'Changed_Expanded_cluster_lists_Ruleset-level', 'Upcoming_Rulesets')
    ECC_service_fldr = pathlib.Path(filepath_dict['publishing_TRUD_fldr'],'Changed_Expanded_cluster_lists_Service-level', 'Upcoming_Services')
    ECC_zip('ECC_changes_UPCOMING_rulesets_and_services',ECC_ruleset_fldr, ECC_service_fldr, filepath_dict, configDates)

    #Create zipfile of all static ECC_ruleset and ECC_service xlsx files
    ECC_ruleset_fldr = pathlib.Path(filepath_dict['publishing_TRUD_fldr'],'Static_Expanded_cluster_lists_Ruleset-level')
    ECC_service_fldr = pathlib.Path(filepath_dict['publishing_TRUD_fldr'],'Static_Expanded_cluster_lists_Service-level')
    ECC_zip('Expanded_Cluster_Contents_list',ECC_ruleset_fldr, ECC_service_fldr, filepath_dict, configDates)
    
    ##Email setup
    #set up config email details
    ECC_email = configEmails['Email_6']

    #Email body, outfile name and specific files detailed for email creation
    body = MIMEText(f"Hi {ECC_email['Addressee']},\n\nPlease find attached the zip file(s) containing the latest updates and changes of codes within Expanded Cluster Content (ECC's) lists, following our latest PCD code release (completed on {configDates.ddMonthYYYY(configDates.PCDreleaseDate)}). \nPlease note, we can only guarantee the content of the PCD refsets; drug/other refsets may not be fully up to date. \n\nKind regards,\n\n", 'plain', 'utf-8')
    SA_outfile_name = (f"DGAT_ECC_changes_post_code_release.eml")
    
    #attach needed files
    zip_file = [PurePath(filepath_dict['publishing_TRUD_fldr'] , f'ECC_changes_LIVE_rulesets_and_services_{configDates.YYYYMMDD(configDates.PCDreleaseDate)}.zip'),
                PurePath(filepath_dict['publishing_TRUD_fldr'] , f'ECC_changes_UPCOMING_rulesets_and_services_{configDates.YYYYMMDD(configDates.PCDreleaseDate)}.zip'),
                PurePath(filepath_dict['publishing_TRUD_fldr'] , f'ECC_changes_summary_file_{configDates.YYYYMMDD(configDates.PCDreleaseDate)}.txt')]

    #final composition of email
    #(dont split up the parameters here, it turns some of them into tuples rather than string!)
    email(email_subject=ECC_email['subject'], email_To=ECC_email['To'], email_Cc=ECC_email['Cc'],email_body=body, outfile_path=filepath_dict['publishing_TRUD_fldr'], outfile_name=SA_outfile_name,  attach=True,  attachment_files=zip_file)

#-------------------------------------------------------------------------------------------------

def create_ECC_summary_txt_file(filename:str, configDates: class_Dates):
    
    """ Creates a summary text file that is used to detail all of the specific changes that have happened at cluster, ruleset and service level. 
    This is to keep seperate from the over scripts logger. """

    file1 = open(filename, "w")
    L = [f"This is a summary sheet of the changes made between the prior ({configDates.PrevPCDrelDate}) and latest ({configDates.PCDreleaseDate}) Snomed Code Release. \n"]
    file1.writelines(L)
    file1.close()

    return(file1)

#-------------------------------------------------------------------------------------------------
    
def create_ECC_fldrs(publishing_TRUD_fldr):
    
    """
    Actions: 
    - Create all of the Expanded Cluster content ouput folders and sub folders.
    - If the folder(s) already exist, then delete this and remake it (even if items exist within the folder)

    Inputs:
    - publishing_TRUD_fldr - root directory for all outputs
   
    """

    #Set paths for folders of all and static ECC_ruleset and ECC_service for xlsx files
    live_static_ECC_ruleset_fldr = PurePath(publishing_TRUD_fldr,'Static_Expanded_cluster_lists_Ruleset-level')
    live_static_ECC_service_fldr = PurePath(publishing_TRUD_fldr,'Static_Expanded_cluster_lists_Service-level')

    #Set paths for folders of the changed ECC_ruleset and ECC_service for xlsx files
    changed_ECC_ruleset_fldr = PurePath(publishing_TRUD_fldr,'Changed_Expanded_cluster_lists_Ruleset-level')
    changed_ECC_service_fldr = PurePath(publishing_TRUD_fldr,'Changed_Expanded_cluster_lists_Service-level')

    #Set paths for sub 'Live' and 'Upcoming' folders
    ECC_ruleset_Live_fldr = PurePath(changed_ECC_ruleset_fldr,'Live_Rulesets')
    ECC_ruleset_Upcoming_fldr = PurePath(changed_ECC_ruleset_fldr,'Upcoming_Rulesets')

    ECC_service_Live_fldr = PurePath(changed_ECC_service_fldr,'Live_Services')
    ECC_service_Upcoming_fldr = PurePath(changed_ECC_service_fldr,'Upcoming_Services')


    #create output folders
    for folder in [live_static_ECC_ruleset_fldr, live_static_ECC_service_fldr, changed_ECC_ruleset_fldr, changed_ECC_service_fldr
                   , ECC_ruleset_Live_fldr, ECC_ruleset_Upcoming_fldr, ECC_service_Live_fldr, ECC_service_Upcoming_fldr ]:
        
        dirpath = Path(folder)
        
        try: 
            #if not exisiting already, create folder
            os.mkdir(dirpath)
        except FileExistsError:
            #folder already exists and is a directory (doesn't matter if populated or empty), delete and recreate anew
            if dirpath.exists() and dirpath.is_dir():
                shutil.rmtree(dirpath)
                os.mkdir(dirpath)

    return changed_ECC_ruleset_fldr, changed_ECC_service_fldr, live_static_ECC_ruleset_fldr, live_static_ECC_service_fldr

#----------------------------------------------------------------------------------

def create_ECC_changed_cluster_list(code_release_all_changes_df):
    
    """ Actions: 
    - runs through the df of all outcomes of the latest code release (added, removed and unchanged).
    - runs through each cluster and determines if there have been any changes to ECC (addded or removed)
    - adds a cluster to the ECC_changed_cluster_list if it does, and ignores if no changes have been made.    """

    # using collections.OrderedDict.fromkeys()
    from collections import OrderedDict

    #convert Cluster_ID column values to list
    all_clusters_list = code_release_all_changes_df['Cluster_ID'].values.tolist()
    #deduplicate and order the list
    all_clusters_list = list(OrderedDict.fromkeys(all_clusters_list))
    
    #run through and filter the df based off each cluster. If cluster only contains 'No change' in the Outcome col then ignore. 
    #if contains 'added' or 'removed', then append cluster to changed list. 
    ECC_changed_cluster_list = []
    write_summary_txt_file(ECC_summary_txt,f"Reviewing which of the {len(all_clusters_list)}, have had changes in the latest code release...")
    logging.info(f"Reviewing which of the {len(all_clusters_list)}, have had changes in the latest code release...")
    
    run_number = 1
    for cluster in all_clusters_list:
        cluster_filtered_df = code_release_all_changes_df.loc[code_release_all_changes_df['Cluster_ID'] == cluster] 
        
        #if the cluster filtered df contains 'added' or 'removed', then it will return these rows. This shows that the cluster ECC has had some changes.
        if string_in_df(cluster_filtered_df, 'Outcome', ['added', 'removed']) == True:   
            ECC_changed_cluster_list.append(cluster)
            write_summary_txt_file(ECC_summary_txt,f"{run_number} - {cluster} reviewed: Cluster changes have been identified. ")
        else: #no changes identified, so skip
            write_summary_txt_file(ECC_summary_txt,f"{run_number} - {cluster} reviewed: No cluster changes identified.")
            pass
        run_number += 1 
        
    write_summary_txt_file(ECC_summary_txt,f"\n Number of clusters found to have had ECC following the latest code release: {len(ECC_changed_cluster_list)} ;  \n {ECC_changed_cluster_list} \n")    
    logging.info(f"Clusters with changes following the latest code release: {len(ECC_changed_cluster_list)}") 

    return ECC_changed_cluster_list

#----------------------------------------------------------------------------------

def copy_bulk_update_ECC_caveat_template(configDates:class_Dates, filepath_dict:dict): 
    """
    Actions: 
    - Make copy of the ECC template and move it to the root ECC outputs folder .
    - Then update the standard text to update dates within cells, completed via the replacementTextKeyPairs dictionary.  """

    #make a copy of the main template and move to the outputs folder
    og_ECC_template = PurePath(os.getcwd() + '\\templates_and_inputs\\ECC_caveats_template_YYYYMMDD.xlsx')                                      #current location
    ECC_updates_template_file = PurePath(filepath_dict['publishing_TRUD_fldr'], f'ECC_caveats_template_{(configDates.PCDreleaseDate)}.xlsx')    #new location
    shutil.copyfile(og_ECC_template,ECC_updates_template_file)

    #specific rows to bulk update the dates in the copied template (yet to find easier way to look for sub-string...)
    replacementTextKeyPairs = {
        'The primary care cluster contents below were correct on the day they were generated (<<YYYYMMDD>>) and were based on the UK SNOMED release from<<UKRelDate>>.' : f'The primary care cluster contents below were correct on the day they were generated ({configDates.ddMonthYYYY(configDates.PCDreleaseDate)}) and were based on the UK SNOMED release from {configDates.ddMonthYYYY(configDates.UKreleaseDate)}.',
        'The drug cluster contents below were based on the UK drugs release from <<DrugUKDate>>.' : f'The drug cluster contents below were based on the UK drugs release from {configDates.ddMonthYYYY(configDates.UKdrugsDate)}.',
        'The cluster contents on <<YYYYMMDD>> were as follows:' : f'The cluster contents on {configDates.ddMonthYYYY(configDates.PCDreleaseDate)} were as follows:'
        }

    #replace the all of the dates within the caveat information (completed 1 row at a time)
    for key, value in replacementTextKeyPairs.items():
        bulk_replace_str_xlsx(filepath_dict['publishing_TRUD_fldr'], f'ECC_caveats_template_{(configDates.PCDreleaseDate)}.xlsx', "Sheet1", key, value)

#----------------------------------------------------------------------------------
def create_save_all_ECC_files(TRUD_fldr_ECCs, configDates: class_Dates, ECC_data:pd.DataFrame, UID:str, serviceORruleset:str, filepath_dict:dict, static_or_changed:str):
    """
    Actions: 
        - Split up the UID into usable vairables; this will be split up once for Service UID and twice for Ruleset UID
        - Update the ECC filename depending on whether it is a changed or static ECC file
        - Make a copy of the newly amended ECC template file (in the root outputs folder) and rename it with the new ECC filename
        - Replace the string in the excel file to update it with the specific UID ECC information 
        - Insert the exported ECC table from SQL and insert it into the newly copied ECC file. Rename the Sheet and save the workbook.

    Inputs:
        - TRUD_fldr_ECCs - folder where the specific ECC output needs to be saved to
        - configDates - Dates dictionary
        - ECC_data - The ECC SQL table that needs to be inserted into the xlsx file
        - UID - string consisting of unique Service_ID, Version Number and Ruleset_ID* (*ruleset ECC's only) 
        - serviceORruleset - string to detail if the ECC has been ran at Service or Ruleset level
        - filepath_dict - Filepaths dictionary 
        - static_or_changed - string to detail if the ECC that's been ran is 'static' or 'changed' (changed is the same as static, however includes "outcome" column and any 'removed' codes)

    Outputs:
        - 1 single ECC file - either Static or Changed; Ruleset (Live or upcoming) or Service (Live only)
    """        

    #split up UID into usable variables
    if serviceORruleset == 'Service':
        Service_ID, Ruleset_version = UID.split('_',1)
        Ruleset_ID = ''
    else:
        Service_ID, Ruleset_ID, Ruleset_version = UID.split('_',2)        
    
    if static_or_changed == 'static':
        ECC_filename = f'{UID}_Expanded_cluster_list_{configDates.YYYYMMDD(configDates.PCDreleaseDate)}.xlsx'
        
    elif static_or_changed == 'changed':
        ECC_filename = f'{UID}_changes_{configDates.YYYYMMDD(configDates.PrevPCDrelDate)}_to_{configDates.YYYYMMDD(configDates.PCDreleaseDate)}.xlsx'
    else:
        write_summary_txt_file(ECC_summary_txt,f'Unsure what type of xlsx format is needed...skipping: {static_or_changed} - {UID}.')
        pass    
    
    # create new copy and rename new ECC file.
    ECC_updates_template_file = PurePath(filepath_dict['publishing_TRUD_fldr'], f'ECC_caveats_template_{(configDates.PCDreleaseDate)}.xlsx')
    ECC_updates_file = PurePath(TRUD_fldr_ECCs, ECC_filename)
    shutil.copyfile(ECC_updates_template_file, ECC_updates_file)
    
    #replace the string in the first row of the caveats section
    line_replacement_text = Service_ID + ' ' + Ruleset_ID + ' v' + Ruleset_version
    bulk_replace_str_xlsx(TRUD_fldr_ECCs, ECC_filename, "Sheet1", '<<item>> Cluster Contents (<<YYYYMMDD>>)', f'{line_replacement_text} Cluster Contents on {configDates.YmdDashes(configDates.PCDreleaseDate)}')
       
    #add df to spreadsheet 
    try:         
        with pd.ExcelWriter(ECC_updates_file, mode='a', engine='openpyxl', if_sheet_exists='overlay') as writer:  #to use 'overlay' need pandas version 1.4.0+
            ECC_data.to_excel(writer, sheet_name=str('Sheet1'), startrow=17, index=False) #add data in row 17   
            logging.info(f'ECC {UID} table (' + serviceORruleset + '-level, ' + static_or_changed +') created in xlsx.')
        
    except:
        logging.error('ERROR: Could not write ' + UID + ' to xlsx (' + serviceORruleset + '-level, ' + static_or_changed +'). This will need to be done manually.')

    # #rename sheet and save
    # new_sheet_name = serviceORruleset + '_ECC_contents' 
    # with pd.ExcelWriter(ECC_updates_file, mode='a', engine='openpyxl', if_sheet_exists='overlay') as writer:  #to use 'overlay' need pandas version 1.4.0+
    #     ss = openpyxl.load_workbook(ECC_updates_file)
    #     ss_sheet = ss['Sheet1']
    #     ss_sheet.title = new_sheet_name #rename sheet
    #     ss.save(ECC_updates_file)     

    #----------------------------------------------------------------------------------
def create_changed_ruleset_expanded_cluster_files(live_upcoming_rulesets, code_release_all_changes_df, changed_ECC_ruleset_fldr, configDates: class_Dates, filepath_dict:dict, UKSNOMEDCT:class_UKSNOMEDCT_dbs, LocalSNOMEDCT:class_LocalSNOMEDCT_dbs, ClusterManagement:class_ClusterManagement_dbs, cluster_rulesets_all):
    """
    Actions:   "save xlsx files of the ECC code comparisons: Ruleset level "
    - identify only the cluster to have had changes applied to them in the latest code release.
    - find all of the rulesets that have thier ECC affected by one of their clusters changing
    - Create a dictionary of the Ruleset: UID for all rulesets that have had at least 1 cluster change made to them and return the latest version 
    - print out logs of to detail which rulesets and cluster have had changes made 
    
    - Ran seperatly for each Live and Upcoming Service of interest;
        - Then for each that Service has had at least one cluster updated in the latest release, execute the SQL ECC query and save down the static ECC in an xlsx file in the outputs folder.
    
    Output:
    - summary information added to the ECC summary text file
    - Each ruleset will have an ECC output and include a column of "Outcome" based off the latest code release. This will include removal codes
    """  
    logging.info('Creating changed ruleset expanded cluster files')
    
    #remove any clusters that have had no changes in the latest release (only have 'no change' codes)
    ECC_changed_cluster_list = create_ECC_changed_cluster_list(code_release_all_changes_df)
    #filter code_release_all_changes_df so that only clusters that have had ECC changes remain
    latest_release_changed_cluster_codes_df = code_release_all_changes_df[code_release_all_changes_df['Cluster_ID'].isin(ECC_changed_cluster_list)]

    ####----------------------------------------
    #run through live_upcoming_rulesets_with_clusters and remove any rows of clusters that are not in the list. reset the index too.
    rulesets_with_cluster_changes = live_upcoming_rulesets[live_upcoming_rulesets['Cluster_ID'].isin(ECC_changed_cluster_list)].reset_index(drop=True) 

    #convert UID column values to list
    changed_rulesets_UID_list = rulesets_with_cluster_changes['UID'].values.tolist()
    #deduplicate and order the list
    changed_rulesets_UID_list = list(OrderedDict.fromkeys(changed_rulesets_UID_list))
    write_summary_txt_file(ECC_summary_txt,f'The following rulesets have been identified to have had clusters changed within their ECC...{len(changed_rulesets_UID_list)}')
    logging.info(f'{len(changed_rulesets_UID_list)} rulesets identified with cluster changes')

    #print the clusters that have had ECC changes for each ruleset (any with no changes are not printed here...)
    for UID in changed_rulesets_UID_list:
        cluster_filtered_df = rulesets_with_cluster_changes.loc[rulesets_with_cluster_changes['UID'] == UID]
        #now create a list of clusters in the UID filtered df
        changed_clusters_in_changed_rulesets_UID_list = cluster_filtered_df['Cluster_ID'].values.tolist()
        changed_clusters_in_changed_rulesets_UID_list = list(OrderedDict.fromkeys(changed_clusters_in_changed_rulesets_UID_list))
        write_summary_txt_file(ECC_summary_txt,f'Ruleset {UID} has had {len(changed_clusters_in_changed_rulesets_UID_list)} clusters changed in the latest code release: {changed_clusters_in_changed_rulesets_UID_list}.')

    ####----------------------------------------
        
    #filter df to each cluster and detail what changes have been made to each
    """ print the types of changes applied to those clusters that have had at least 1 "added" or "removed" action. """
    write_summary_txt_file(ECC_summary_txt, f'\n Detailing the number of code type changes made to clusters with at least 1 code that has been "added" or "removed".')
    
    
    #filter the overall changes df to that of each cluster, then calculate the quantity of the different types of changes made to each cluster.
    for cluster in ECC_changed_cluster_list:
        cluster_ECC = latest_release_changed_cluster_codes_df.loc[latest_release_changed_cluster_codes_df['Cluster_ID']== cluster]

        ruleset_codes_additions = (cluster_ECC['Outcome'].values == 'added').sum()
        ruleset_codes_removals = (cluster_ECC['Outcome'].values == 'removed').sum()
        ruleset_codes_unchanged = (cluster_ECC['Outcome'].values == 'no change').sum()

        write_summary_txt_file(ECC_summary_txt,f'{cluster} - Number of additions: ' + str(f'{ruleset_codes_additions}'))
        write_summary_txt_file(ECC_summary_txt,f'{cluster} - Number of removals: ' + str(f'{ruleset_codes_removals}'))
        write_summary_txt_file(ECC_summary_txt,f'{cluster} - Number of no changes: ' + str(f'{ruleset_codes_unchanged} \n'))

    ####----------------------------------------

    #run through each UID in the rulesets_with_cluster_changes, and execute the SQL query that returns individual ruleset ECC. Save as an excel file in outputs       
        #convert df with repetitive keys (UID) to a dictionary, i.e. Produces dictionary with UID as a key and if it is 'Live' or 'upcoming' as the value
    all_ECC_changed_UID_df = live_upcoming_rulesets[live_upcoming_rulesets['UID'].isin(changed_rulesets_UID_list)].reset_index(drop=True) 
    all_ECC_changed_UID_dict = all_ECC_changed_UID_df.set_index('UID')['Stage'].to_dict()

    #count number of times 'live' and 'upcoming' are used
    write_summary_txt_file(ECC_summary_txt,f"Number of Live outputs expected: {countOf(all_ECC_changed_UID_dict.values(),'Live')}") 
    write_summary_txt_file(ECC_summary_txt,f"Number of Upcoming outputs expected: {countOf(all_ECC_changed_UID_dict.values(),'Upcoming')}") 
    write_summary_txt_file(ECC_summary_txt,f'Number of ruleset outputs to be saved: {len(all_ECC_changed_UID_dict)} - The following Rulesets saved are those identified to have had changes made to their expanded cluster lists. Any not present have had no changes made.')

    logging.info(f"Number of Live outputs expected: {countOf(all_ECC_changed_UID_dict.values(),'Live')}") 
    logging.info(f"Number of Upcoming outputs expected: {countOf(all_ECC_changed_UID_dict.values(),'Upcoming')}")
    logging.info(f'Number of ruleset outputs to be saved: {len(all_ECC_changed_UID_dict)}')


    x = 1
    for UID, stage in all_ECC_changed_UID_dict.items():
        
        #assign vairables based of the UID
        Service_ID, Ruleset_ID, Ruleset_version = UID.split('_',2)
                        
        #execute the ECC ruleset SQL query
        individual_ruleset_ECC = changed_with_outcomes_ruleset_ECC_tbl_SQL_ALTERNATIVE(code_release_all_changes_df, Ruleset_ID, Ruleset_version, Service_ID, cluster_rulesets_all, filepath_dict)

        #SQL_query = sql_query_changed_with_outcomes_ruleset_ECC_tbl(Ruleset_ID, Ruleset_version, Service_ID, LocalSNOMEDCT, UKSNOMEDCT, ClusterManagement, configDates)
#
        #individual_ruleset_ECC = get_df_from_sql(server=LocalSNOMEDCT.server,                                      
        #                                    database=LocalSNOMEDCT.db,
        #                                    query=SQL_query)  
        
        if individual_ruleset_ECC.empty:
            #logging.warning(f'individual_ruleset_ECC table is empty. Please check that the correct dates / tables have been used in this query. \n {SQL_query} \n')
            logging.warning(f'individual_ruleset_ECC table is empty for {Service_ID} {Ruleset_ID} v{Ruleset_version}.')
        
        #save the outputs
        if stage == 'Live':
            create_save_all_ECC_files(PurePath(changed_ECC_ruleset_fldr,'Live_Rulesets'), configDates, individual_ruleset_ECC, UID, 'Ruleset', filepath_dict, static_or_changed = 'changed')
        else:
            create_save_all_ECC_files(PurePath(changed_ECC_ruleset_fldr,'Upcoming_Rulesets'), configDates, individual_ruleset_ECC, UID, 'Ruleset', filepath_dict, static_or_changed = 'changed')    

        write_summary_txt_file(ECC_summary_txt,f'{x} - Outputs have been saved for the following Ruleset: {stage}: {Service_ID}, {Ruleset_ID}, {Ruleset_version}')
        x +=1
    
    write_summary_txt_file(ECC_summary_txt,'\n All changed ECC Ruleset outputs have been queried and saved. \n')        

#----------------------------------------------------------------------------------

def create_changed_service_expanded_cluster_files(all_published_ECC_info_services_grouped, stage,service_stage_sub_folder, changed_ECC_service_fldr,  configDates: class_Dates, filepath_dict:dict, UKSNOMEDCT:class_UKSNOMEDCT_dbs, LocalSNOMEDCT:class_LocalSNOMEDCT_dbs, ClusterManagement:class_ClusterManagement_dbs, code_release_all_changes_df:pd.DataFrame, cluster_rulesets_all:pd.DataFrame):
    """
    Actions: 
    - filter the live_upcoming_rulesets df to only live services (of interest).
    - From this create a dictionary of the Service: Version and return the latest version should there be a list of different ruleset versions 
    - Then for each Service, execute the SQL ECC query and save down the static ECC in an xlsx file in the outputs folder

    """
    
    #Live or upcoming services with ECC ouputs only
    Changed_ECC_Services = all_published_ECC_info_services_grouped[all_published_ECC_info_services_grouped['Stage'] == stage]
    
    if Changed_ECC_Services.empty:
        logging.info(f'No {stage} services have been identified to have any cluster or ECC changes.')
        write_summary_txt_file(ECC_summary_txt,f'No {stage} services have been identified to have any cluster or ECC changes. \n')
        pass
    else:
        #use .groupby when wanting to return the value as a list of values found for the same key
        Changed_ECC_Services_dict = Changed_ECC_Services.groupby('Service_ID')['Ruleset_Version'].apply(list).to_dict()        
        
        #We want to only return the highest version numbers for 'live' and 'upcoming'. 
        #If for example there are v5.1 and v5.2 domains within the NCD service, SA_services dictionary above would list both of these and later create seperate ECC's for v5.1 and v5.2 NCD.
        #sort out the key and value lists
        res = dict()
        for key in sorted(Changed_ECC_Services_dict):
            res[key] = sorted(Changed_ECC_Services_dict[key], reverse=True)

        #Slice till Kth UID within a dictionary value list
        K = 1
        Changed_ECC_Services_dict = slice_dict_list(res, K)   #resets the dictionary key values, so that only the first K indexes are returned 
                
        #We need to floor the version number as one service could validly have multiple different version numbers for its different rulesets
        rounded_changed_ECC_services_dict = {i:math.floor(j[0]) for i,j in Changed_ECC_Services_dict.items() }
        
        logging.info(f'{stage} services with changes since the latest code release: {len(rounded_changed_ECC_services_dict)}')
        write_summary_txt_file(ECC_summary_txt,f'{stage} services that have had ECC changes since the latest code release: {rounded_changed_ECC_services_dict}')
        
        #turn value from a list to a string: ['example'] >> 'example'  
        #for each service ID and ruleset version pairing in the Changed_ECC_Services_dict dictionary, change the version number to a string. 
        Changed_ECC_Services_dict = {i:str(j[0]) for i,j in Changed_ECC_Services_dict.items() }

        x = 1
        for Service_ID, service_version in Changed_ECC_Services_dict.items():
            
            #filter ECC by service and version
            #try:
            #    #execute the ECC service-level SQL query
            #    services_full_ECC_table = get_df_from_sql(server=LocalSNOMEDCT.server,
            #                                    database=UKSNOMEDCT.db,
            #                                    query=sql_query_changed_with_outcomes_service_ECC_tbl(str(service_version), Service_ID, UKSNOMEDCT, ClusterManagement, LocalSNOMEDCT, configDates))
            #except:
            services_full_ECC_table = changed_with_outcomes_service_ECC_tbl_SQL_ALTERNATIVE(code_release_all_changes_df, service_version, Service_ID, cluster_rulesets_all)
                                  

            if services_full_ECC_table.empty:
                logging.warning(f'{stage}_services_full_ECC_table is empty. Please check that the correct dates / tables have been used in this query. \n')

            #save the outputs
            rounded_version = rounded_changed_ECC_services_dict[Service_ID]

            UID = Service_ID + "_" + str(rounded_version)
            create_save_all_ECC_files(PurePath(changed_ECC_service_fldr, service_stage_sub_folder), configDates, services_full_ECC_table, UID, 'Service', filepath_dict, static_or_changed = 'changed')    

            write_summary_txt_file(ECC_summary_txt,f'{x} - Outputs have been saved for the following Service: {stage}; {Service_ID},{service_version}.')
            x +=1
        
        write_summary_txt_file(ECC_summary_txt,f'\n All {stage} changed ECC Service outputs have been queried and saved. \n')

        return all_published_ECC_info_services_grouped



#----------------------------------------------------------------------------------
def produce_save_service_changed_ECC_files(live_upcoming_rulesets, ecc_service_list, changed_ECC_service_fldr,  configDates: class_Dates, filepath_dict:dict, UKSNOMEDCT:class_UKSNOMEDCT_dbs, LocalSNOMEDCT:class_LocalSNOMEDCT_dbs, ClusterManagement:class_ClusterManagement_dbs,code_release_all_changes_df:pd.DataFrame, cluster_ruleset_all:pd.DataFrame):
    """
    Actions: 
    - filter the live_upcoming_rulesets df to only live services (of interest).
    - From this create a dictionary of the Service: Version and return the latest version should there be a list of different ruleset versions 
    - Ran seperatly for each Live and Upcoming Service of interest;
        - Then for each that Service has had at least one cluster updated in the latest release, execute the SQL ECC query and save down the static ECC in an xlsx file in the outputs folder.
    
    Output:
    - Each service will have an ECC output and include a column of "Outcome" based off the latest code release. This will include removal codes.

    """

    #return df of only 'Service_ID' and 'Ruleset_Version' columns
    all_published_ECC_info_services = live_upcoming_rulesets[['Stage','Service_ID','Ruleset_Version']]

    #return only unique rows - rop duplicates
    all_published_ECC_info_services_unique = all_published_ECC_info_services.drop_duplicates()

    #filter df so that only the intereseted services are returned
    all_published_ECC_info_services_grouped = all_published_ECC_info_services_unique[all_published_ECC_info_services_unique['Service_ID'].isin(ecc_service_list)].reset_index(drop=True) 

    #Now split up the outputs by 'Live' and 'upcoming'"

        #Live services with ECC ouputs only
    create_changed_service_expanded_cluster_files(all_published_ECC_info_services_grouped, 'Live','Live_Services', changed_ECC_service_fldr, configDates, filepath_dict, UKSNOMEDCT, LocalSNOMEDCT, ClusterManagement,code_release_all_changes_df, cluster_ruleset_all)

        #upcoming services with ECC ouputs only
    create_changed_service_expanded_cluster_files(all_published_ECC_info_services_grouped, 'Upcoming','Upcoming_Services', changed_ECC_service_fldr, configDates, filepath_dict, UKSNOMEDCT, LocalSNOMEDCT, ClusterManagement, code_release_all_changes_df, cluster_ruleset_all)

#----------------------------------------------------------------------------------

def static_ruleset_ECC_create_save_loop(live_upcoming_rulesets, live_static_ECC_ruleset_fldr, configDates: class_Dates, filepath_dict:dict, UKSNOMEDCT:class_UKSNOMEDCT_dbs, LocalSNOMEDCT:class_LocalSNOMEDCT_dbs, ClusterManagement:class_ClusterManagement_dbs):
    """
    Actions: 
    - filter the live_upcoming_rulesets df to only live services (of interest).
    - From this create a dictionary of the Ruleset: Version and return the latest version should there be a list of different ruleset versions 
    - Ran only for Live rulesets;
        - Then for each ruleset that has had at least one cluster updated in the latest release, execute the SQL ECC query and save down the static ECC in an xlsx file in the outputs folder.
    
    Output:
    - Each ruleset will have a static "as is" ECC output. This will not include a column of "Outcome" based off the latest code release or contain any removal codes.

    """

    #filter df so that only live rulesets are returned
    all_live_rulesets_df = live_upcoming_rulesets[live_upcoming_rulesets['Stage'] == 'Live'].reset_index(drop=True) 
    #convert Ruleset_ID and UID column into a dictionary
    all_live_rulesets_dict = all_live_rulesets_df.set_index('UID')['Ruleset_ID'].to_dict()
    all_live_rulesets_dict = all_live_rulesets_df.groupby('UID')['Ruleset_ID'].apply(list).to_dict() 

    write_summary_txt_file(ECC_summary_txt,f'Expecting {len(all_live_rulesets_dict)} Live ruleset(s) to have static ECC outputs to be saved.')
   
    x = 1
    for UID, ruleset in all_live_rulesets_dict.items():
        #assign vairables based of the UID
        Service_ID, Ruleset_ID, Ruleset_version = UID.split('_',2)

        #check if a copy of this ruleset has already been created (from the changed ECC, minus the 'Outcome' column - otherwise the exact same)
        static_ruleset_ECC_filename = f'{Service_ID}_{Ruleset_ID}_{Ruleset_version}_changes_{configDates.YYYYMMDD(configDates.PCDreleaseDate)}_PCD_code_release.xlsx' ###consider function to reduce size of filename if over xx characters long? 
        if os.path.exists(PurePath(live_static_ECC_ruleset_fldr, static_ruleset_ECC_filename)) == True:
            print(f'file already exists, so skipping: {UID}')
            pass
        else:
            #run sql query of unchanged ruleset to return the ECC
            #execute the ECC ruleset SQL query
            SQL_query = sql_query_static_ruleset_ECC_tbl(Ruleset_ID, Ruleset_version, Service_ID, LocalSNOMEDCT, UKSNOMEDCT, ClusterManagement)

            static_individual_ruleset_ECC = get_df_from_sql(server=LocalSNOMEDCT.server,
                                                database=LocalSNOMEDCT.db,
                                                query=SQL_query)  

            if static_individual_ruleset_ECC.empty:
                logging.warning(f'static_individual_ruleset_ECC table is empty. Please check that the correct dates / tables have been used in this query. \n {SQL_query} \n')

            #save the outputs
            create_save_all_ECC_files(live_static_ECC_ruleset_fldr, configDates, static_individual_ruleset_ECC, UID, 'Ruleset', filepath_dict, static_or_changed = 'static')

            write_summary_txt_file(ECC_summary_txt,f'{x} - Static ECC Outputs have been saved for the following Ruleset: {Service_ID}, {Ruleset_ID}, {Ruleset_version}')
            x +=1
#----------------------------------------------------------------------------------

def static_service_ECC_create_save_loop(live_upcoming_rulesets, ecc_service_list, live_static_ECC_service_fldr, configDates: class_Dates, filepath_dict:dict, UKSNOMEDCT:class_UKSNOMEDCT_dbs, LocalSNOMEDCT:class_LocalSNOMEDCT_dbs, ClusterManagement:class_ClusterManagement_dbs):
    """
    Actions: 
    - filter the live_upcoming_rulesets df to only live services (of interest).
    - From this create a dictionary of the Service: Version and return the latest version should there be a list of different ruleset versions
    - Run only for Live Service of interest;
        - Then for each that Service has had at least one cluster updated in the latest release, execute the SQL ECC query and save down the static ECC in an xlsx file in the outputs folder.
    
    Output:
    - Each service will have a static "as is" ECC output. This will not include a column of "Outcome" based off the latest code release or contain any removal codes.

    """
        
    #return df of only 'Stage', 'Service_ID' and 'Ruleset_Version' columns
    all_published_ECC_info_services = live_upcoming_rulesets[['Stage','Service_ID','Ruleset_Version']]

    #filter df so that only live services are returned
    all_published_ECC_info_services = all_published_ECC_info_services[all_published_ECC_info_services['Stage'] == 'Live'].reset_index(drop=True)

    #return only unique rows - rop duplicates
    all_published_ECC_info_services_unique = all_published_ECC_info_services.drop_duplicates()

    #filter df so that only the intereseted services are returned
    all_published_ECC_info_services_grouped = all_published_ECC_info_services_unique[all_published_ECC_info_services_unique['Service_ID'].isin(ecc_service_list)].reset_index(drop=True) 
     
    
        #check that live services with ECC  have been identified
    if all_published_ECC_info_services_grouped.empty:
        write_summary_txt_file(ECC_summary_txt,f'No live services have been identified to create ECC...Please investigate.')
        pass
    else:
        #convert 'Ruleset_Version' column into string to allow it to turn into a dataframe
        all_published_ECC_info_services_grouped['Ruleset_Version'] = all_published_ECC_info_services_grouped['Ruleset_Version'].astype('string')

        #convert Service_ID and Ruleset_Version column into a dictionary
        all_live_services_dict = all_published_ECC_info_services_grouped.groupby('Service_ID')['Ruleset_Version'].apply(list).to_dict()

        write_summary_txt_file(ECC_summary_txt,f'Expecting {len(all_live_services_dict)} Live service(s) to have static ECC outputs to be saved.')
        
        #We want to only return the highest version numbers for 'live'.
        res = dict()
        for key in sorted(all_live_services_dict):
            res[key] = sorted(all_live_services_dict[key], reverse=True)

        #Slice till Kth UID within a dictionary value list
        K = 1
        all_live_services_dict = slice_dict_list(res, K)   #resets the dictionary key values, so that only the first 2 indexes are returned 
        #turn value from a list to a string: ['example'] >> 'example'  
        all_live_services_dict = {i:str(j[0]) for i,j in all_live_services_dict.items() }
        write_summary_txt_file(ECC_summary_txt,f'Live services that are to export the latest ECC: {all_live_services_dict}')

        x = 1
        for Service_ID, service_version in all_live_services_dict.items():
            #execute the ECC ruleset SQL query
            services_full_ECC_table = get_df_from_sql(server=LocalSNOMEDCT.server,
                                                database=UKSNOMEDCT.db,
                                                query=sql_query_static_service_ECC_tbl(service_version, Service_ID, LocalSNOMEDCT, UKSNOMEDCT, ClusterManagement))


            if services_full_ECC_table.empty:
                logging.warning(f'{Service_ID}_{service_version}_services_full_ECC_table is empty. Please check that the correct dates / tables have been used in this query. \n')

            #save the outputs
            UID = Service_ID + "_" + str(service_version)
            create_save_all_ECC_files(live_static_ECC_service_fldr, configDates, services_full_ECC_table, UID, 'Service', filepath_dict, static_or_changed = 'static')    

            write_summary_txt_file(ECC_summary_txt,f'{x} - Static ECC Outputs have been saved for the following Service: Live; {Service_ID},{service_version}.')
            x +=1
        
        write_summary_txt_file(ECC_summary_txt,'\n All static ECC service outputs have been queried and saved. \n')
        
        return all_published_ECC_info_services_grouped

#----------------------------------------------------------------------------------

def expanded_clusters_changes_full_funx(config:dict, configDates: class_Dates, configEmails:dict, filepath_dict:dict, UKSNOMEDCT:class_UKSNOMEDCT_dbs,
                                         LocalSNOMEDCT:class_LocalSNOMEDCT_dbs, ClusterManagement:class_ClusterManagement_dbs):
    #runs all of the functions to execute the whole ECC updates stage

    """
    Actions: 
    - et up for actions; create ECC summary txt file and create ouput folders
    - find all clusters that have changed in the latest code release (A)
    - find all of the latest rulesets and the clusters they use (B)
    -  find out which rulesets / services are affected (using A and B)
        - *repeat the below for all rulesets and then all services identified to be affected in a loop
        - produce a summary of the codes in the clusters that have changed (adds and removes)
        - Run ECC changes outputs for all rulesets and services alike (containing "outcomes" and removed codes)
        - run additional ECC SQL queries to have the Live Static Expanded cluster contents for rulesets and services (does not contain  "outcomes" or removed codes)
    - zip up output files
    - create and send an email to SA with changes. 

    """

    #1)Create folders
    # SetUp)
    global ECC_summary_txt
    logging.info(f'Running the ECC updates stage...')
  
    #create an individual ECC summary text file - additional information about the ECC's, seperate to the logger
    ECC_summary_txt = str(PurePath(filepath_dict['publishing_TRUD_fldr'], f'ECC_changes_summary_file_{configDates.YYYYMMDD(configDates.PCDreleaseDate)}.txt'))
    create_ECC_summary_txt_file(ECC_summary_txt, configDates)

    #create the ruleset and service output folders - if folder already exists, deletes this and then recreates it 
    changed_ECC_ruleset_fldr, changed_ECC_service_fldr,  live_static_ECC_ruleset_fldr, live_static_ECC_service_fldr = create_ECC_fldrs(filepath_dict['publishing_TRUD_fldr'])

    logging.info('Begin identifying SNOMED changes')
    #----------------------------------------------------------------------------------
    #) create df of all of the snomed code changes that have occured in the latest code release
    code_release_all_changes_df, cluster_ruleset_all = identify_all_cluster_changes_SQL_ALTERNATIVE(LocalSNOMEDCT, UKSNOMEDCT, ClusterManagement, configDates)
    logging.info('Initial SNOMED change idenitification complete')
    
    logging.info('Identifying rulesets')
    #)Return of all current active and published rulesets, with a column specifying if it is 'live' or 'upcoming'  
    live_upcoming_rulesets = get_df_from_sql(server=LocalSNOMEDCT.server,
                                                database=ClusterManagement.db,
                                                query=all_live_upcoming_rulesets_with_clusters(ClusterManagement))  
    
    #set the Ruleset_Version from object to numeric
    live_upcoming_rulesets["Ruleset_Version"] = live_upcoming_rulesets[["Ruleset_Version"]].apply(pd.to_numeric)
    #create the UID column from a combination of the ruleset info
    live_upcoming_rulesets['UID'] = live_upcoming_rulesets.apply(lambda row: f"{row['Service_ID']}_{row['Ruleset_ID']}_{str(row['Ruleset_Version'])}", axis=1)
    logging.info('Rulesets identified')
     #----------------------------------------------------------------------------------
    #) Create a copy of the ECC caveats template and bulk update the dates in it
    copy_bulk_update_ECC_caveat_template(configDates, filepath_dict)   

    #----------------------------------------------------------------------------------
    #) Run the changed Rulesets ECC loops
    create_changed_ruleset_expanded_cluster_files(live_upcoming_rulesets, code_release_all_changes_df, changed_ECC_ruleset_fldr, configDates, filepath_dict, UKSNOMEDCT, LocalSNOMEDCT, ClusterManagement, cluster_ruleset_all)
    #----------------------------------------------------------------------------------
    #) Run the changed Services ECC loops
    logging.info('Generating changed outputs...')
    ecc_service_list = config['Expanded_cluster_lists_Service-level']['ecc_service_list']
    produce_save_service_changed_ECC_files(live_upcoming_rulesets, ecc_service_list, changed_ECC_service_fldr, configDates, filepath_dict, UKSNOMEDCT, LocalSNOMEDCT, ClusterManagement,code_release_all_changes_df, cluster_ruleset_all)
    logging.info('Changed outputs complete')
    #----------------------------------------------------------------------------------
    #) Run the static all rulesets ECC loops
    logging.info('Generating static outputs...')
    write_summary_txt_file(ECC_summary_txt,'Static ECC Outputs are now being created and saved for the all live rulesets and services.')
 
    static_ruleset_ECC_create_save_loop(live_upcoming_rulesets, live_static_ECC_ruleset_fldr, configDates, filepath_dict, UKSNOMEDCT, LocalSNOMEDCT, ClusterManagement)
    logging.info('Static ruleset outputs complete...')
    #----------------------------------------------------------------------------------
    #) Run the static all services ECC loops
    static_service_ECC_create_save_loop(live_upcoming_rulesets, ecc_service_list, live_static_ECC_service_fldr, configDates, filepath_dict, UKSNOMEDCT, LocalSNOMEDCT, ClusterManagement)
    logging.info('Static service outputs complete...')
    #----------------------------------------------------------------------------------
    #)zip up changed ECC outputted documents and create emails
    ECC_zip_and_email(configEmails, filepath_dict, configDates)

    #----------------------------------------------------------------------------------
    #)round off function
    write_summary_txt_file(ECC_summary_txt,f'Completed the ECC updates stage.')
    logging.info(f'Completed the ECC updates stage.')



#-------------------------------------------------------------------------------------------------------------------------------------------
def identify_all_cluster_changes_SQL_ALTERNATIVE(LocalSNOMEDCT, UKSNOMEDCT, ClusterManagement, configDates, ):
    '''
    
    '''
    #Pull dataframes needed from SQL...

    #code info
    descriptions = get_df_from_sql(server=UKSNOMEDCT.server, database=UKSNOMEDCT.db, query=sql_query_select_all_short(UKSNOMEDCT.SCT_DESCRIPTION)) 
    concepts = get_df_from_sql(server=UKSNOMEDCT.server, database=UKSNOMEDCT.db, query=sql_query_select_all_short(UKSNOMEDCT.SCT_CONCEPT)) 
    #clusters
    clusters_all = get_df_from_sql(server=LocalSNOMEDCT.server, database=ClusterManagement.db, query=sql_query_select_all_short(ClusterManagement.Clusters)) 
    cluster_ruleset_all = get_df_from_sql(server=LocalSNOMEDCT.server, database=ClusterManagement.db, query=sql_query_select_all_short(ClusterManagement.Cluster_Ruleset))
    ruleset_all = get_df_from_sql(server=LocalSNOMEDCT.server, database=ClusterManagement.db, query=sql_query_select_all_short(ClusterManagement.Rulesets))  
    #old and new snapshot primary care refset tables
    new_PCD_all  = get_df_from_sql(server=LocalSNOMEDCT.server, database=LocalSNOMEDCT.db, query=sql_query_select_all(LocalSNOMEDCT.db, f'der2_Refset_SimpleSnapshot_1000230_{configDates.PCDreleaseDate}')) 
    old_PCD_all  = get_df_from_sql(server=LocalSNOMEDCT.server, database=LocalSNOMEDCT.db, query=sql_query_select_all(LocalSNOMEDCT.db, f'der2_Refset_SimpleSnapshot_1000230_{configDates.PrevPCDrelDate}'))
    #old and new snapshot monolith refset tables
    print('prior_db = ', UKSNOMEDCT.prior_db)

    old_MONO_all = get_df_from_sql(server=UKSNOMEDCT.server, database=UKSNOMEDCT.prior_db, query=sql_query_select_all(UKSNOMEDCT.prior_db, f'der2_Refset_SimpleMONOSnapshot_GB_{configDates.PrevUKrelDate}')) 
    new_MONO_all = get_df_from_sql(server=UKSNOMEDCT.server, database=UKSNOMEDCT.db, query=sql_query_select_all(UKSNOMEDCT.db, f'der2_Refset_SimpleMONOSnapshot_GB_{configDates.UKreleaseDate}')) 
    #ensure no pcd content in monolith
    old_MONO_all = old_MONO_all.loc[old_MONO_all['moduleId']!='999000011000230102']
    new_MONO_all = new_MONO_all.loc[new_MONO_all['moduleId']!='999000011000230102']
    logging.info('Data imported from SQL')
    #---------------------------------------------------

    #join clusters and rulesets to identify cluster ID, description and refset IDs for all clusters that have ever been used in a ruleset
    clusters_info = pd.merge(clusters_all, cluster_ruleset_all, how='inner', left_on='Cluster_ID',right_on='Cluster_ID')
    #filter for active clusters
    clusters_info = clusters_info.loc[clusters_info['Cluster_Date_Inactive'].isnull()]
    #only include 3 columns
    clusters_info = clusters_info[['Cluster_ID','Cluster_Description','Refset_ID']].drop_duplicates()
    logging.info('Relevant clusters identified')
    #---------------------------------------------------
    #concatenate to create old and new combined content    
    old_content = pd.concat([old_PCD_all, old_MONO_all])
    #filter to only include codes in refset (active=1)
    old_content = old_content.loc[old_content['active']=='1']
    #bring in cluster details and only include refset contents for clusters of interest 
    old_content = pd.merge(old_content, clusters_info,how='inner',left_on='refsetId',right_on='Refset_ID')

    new_content = pd.concat([new_PCD_all, new_MONO_all])
    #filter to only include codes in refset (active=1)
    new_content = new_content.loc[new_content['active']=='1']
    #bring in cluster details and only include refset contents for clusters of interest 
    new_content = pd.merge(new_content, clusters_info,how='inner',left_on='refsetId',right_on='Refset_ID')

    logging.info('Old and new refset content identified')
    #---------------------------------------------------
    #join to identify additions and removals
    changes_df = pd.merge(old_content, new_content, how='outer', left_on=['referencedComponentId','Cluster_ID'],right_on=['referencedComponentId','Cluster_ID'],indicator=True)
    
    #Set cluster, description, refset and code values
    changes_df['Cluster_Description'] = np.select(changes_df['_merge']=='right_only', changes_df['Cluster_Description_y'], default=changes_df['Cluster_Description_x'])
    changes_df['Refset_ID']           = np.select(changes_df['_merge']=='right_only', changes_df['Refset_ID_y'], default=changes_df['Refset_ID_x'])
    changes_df['SNOMED_concept_ID']   = changes_df['referencedComponentId']
    #create outcome column based on whether code/cluster combo was in old, new or both releases
    changes_df['Outcome'] = np.select([changes_df['_merge']=='right_only'
                                       ,changes_df['_merge']=='left_only'
                                       ,changes_df['_merge']=='both']
                                      , ['added','removed','no change']
                                      , default='Unknown')
    #only keep generated columns
    change_summary = changes_df[['Cluster_ID','Cluster_Description','Refset_ID','SNOMED_concept_ID','Outcome']]
    logging.info('SNOMED Changes identified')

    #---------------------------------------------------
    #produce final formatted dataframe
    
    #limit code descriptions to active fully specified names
    activeFSN = descriptions.loc[descriptions['active']=='1']
    activeFSN = activeFSN.loc[activeFSN['DescriptionType']=='F']
    #add code descriptions to changes
    changes_with_descriptions = pd.merge(change_summary,activeFSN,how='left',left_on='SNOMED_concept_ID',right_on='conceptId')
    changes_with_descriptions['Code_description'] = changes_with_descriptions['term']
    #only keep relevant columns
    changes_with_descriptions = changes_with_descriptions[['Cluster_ID','Cluster_Description','SNOMED_concept_ID','Code_description','Refset_ID','Outcome']]

    #add code active status
    changes_status = pd.merge(changes_with_descriptions,concepts,how='left',left_on='SNOMED_concept_ID',right_on='id')
    changes_status['Active_status'] = changes_status['active']
    #only keep relevant columns
    changes_status = changes_status[['Cluster_ID','Cluster_Description','SNOMED_concept_ID','Code_description','Refset_ID','Active_status','Outcome']]
    
    #reformat refset ID - create 2 new columns
    changes_status['Type_of_Refset'] = np.select(
                                        [changes_status['Refset_ID'].str.contains('100000110')
                                       ,changes_status['Refset_ID'].str.contains('100023010')]
                                      , ['Drug Refset','PCD Refset']
                                      , default='Refset')
    changes_status['Cluster_Code_String'] = '^' + changes_status['Refset_ID'].astype('str')
    
    #order columns and sort
    formatted_changes = changes_status[['Cluster_ID','Cluster_Description','SNOMED_concept_ID','Code_description','Type_of_Refset','Cluster_Code_String','Active_status','Outcome']]
    formatted_changes.sort_values(by=['Outcome','Cluster_ID'])
    formatted_changes = formatted_changes.drop_duplicates()

    logging.info('Formatted expanded cluster dataframe finished.')
    return formatted_changes, cluster_ruleset_all


def changed_with_outcomes_service_ECC_tbl_SQL_ALTERNATIVE(code_release_all_changes_df, service_version, service_ID, cluster_rulesets_all):
    '''
    Actions:
    Merges expanded cluster contents with all clusters in rulesets.
    Takes service version as an integer and returns all cluster contents for rulesets within that service version integer.
    Returns necessary columns ordered by cluster.
    '''
               
    ECC_per_ruleset = pd.merge(code_release_all_changes_df, cluster_rulesets_all, how='left',left_on='Cluster_ID',right_on='Cluster_ID')
    
    #filter by service ID
    filtered_ECC = ECC_per_ruleset.loc[ECC_per_ruleset['Service_ID']==service_ID]
    
    #filter by ruleset version
    int_version = math.floor(Decimal(service_version))
    filtered_ECC = filtered_ECC.loc[(filtered_ECC['Ruleset_Version']>=int_version) & (filtered_ECC['Ruleset_Version']<(int_version+1))]
    filtered_ECC['Ruleset_Version'] = int_version
    
    #Add coding ID column
    filtered_ECC['Coding_ID'] = 'SNOMED CT'
    
    #select columns and order
    filtered_ECC = filtered_ECC[['Service_ID','Ruleset_Version','Coding_ID','Cluster_ID','Cluster_Description','SNOMED_concept_ID','Code_description','Type_of_Refset','Cluster_Code_String','Active_status','Outcome']]    
    filtered_ECC.sort_values(by=['Cluster_ID'])
    filtered_ECC = filtered_ECC.drop_duplicates()

    return filtered_ECC



def changed_with_outcomes_ruleset_ECC_tbl_SQL_ALTERNATIVE(code_release_all_changes_df, ruleset_ID, ruleset_version, service_ID, cluster_rulesets_all, filepath_dict:dict):
    '''
    Actions:
    Merges expanded cluster contents with all clusters in rulesets.
    Takes ruleset version and returns all cluster contents for that version of the supplied ruleset.
    Returns necessary columns ordered by cluster.
    '''
      
    ECC_per_ruleset = pd.merge(code_release_all_changes_df, cluster_rulesets_all, how='left',left_on='Cluster_ID',right_on='Cluster_ID')

    #filter by service ID
    filtered_ECC = ECC_per_ruleset.loc[ECC_per_ruleset['Service_ID']==service_ID]

    #filter by ruleset ID
    filtered_ECC = filtered_ECC.loc[filtered_ECC['Ruleset_ID']==ruleset_ID]

    #filter by ruleset version
    filtered_ECC.Ruleset_Version = filtered_ECC.Ruleset_Version.astype(float) ####JP: 03122024
    ruleset_version = float(ruleset_version)
    filtered_ECC = filtered_ECC.loc[filtered_ECC['Ruleset_Version']==ruleset_version]
    
    #Add coding ID column
    filtered_ECC['Coding_ID'] = 'SNOMED CT'
    
    #select columns and order
    filtered_ECC = filtered_ECC[['Service_ID','Ruleset_Version','Coding_ID','Cluster_ID','Cluster_Description','SNOMED_concept_ID','Code_description','Type_of_Refset','Cluster_Code_String','Active_status','Outcome']]    
    filtered_ECC.sort_values(by=['Cluster_ID'])
    filtered_ECC = filtered_ECC.drop_duplicates()

    return filtered_ECC
