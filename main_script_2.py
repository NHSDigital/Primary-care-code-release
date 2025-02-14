import os
import shutil
import logging
import pandas as pd
from pathlib import PurePath
from pathlib import Path
from src.utils.dates import class_Dates
from src.utils.filepaths import filepaths
from src.utils.databases import set_dbs_live_or_test, replace_db_dates_config_params
from src.utils.setup_fxns import log_setup, get_config, create_output_folder, move_outputs, replace_email_names_config_params, find_previous_defined_dates
from src.release_stages.create_pcd_tables import create_tables
from src.release_stages.export_tables import export_tables
from src.release_stages.update_SCT import update_SCT_tables
from src.release_stages.Module_dependency import mod_dep_dates
from src.release_stages.Supporting_Products import supportingProducts
from src.release_stages.supporting_info import supportingInfo
from src.release_stages.ukpcsct2_zip import ukpcsct2_zip_upload
from src.release_stages.TRUDpack_request import TRUD_pack_req
from src.release_stages.GDPPR_SPL_content import GDPPR_SPL_content
from src.release_stages.update_Ruleset_Published import update_RP
from src.release_stages.expanded_clusters_changes import expanded_clusters_changes_full_funx


"""
Setup

- a logging file is created for run outputs and times, and any warnings or errors will be flagged here
- user inputs are read in from the config file
- additional dates and filepaths are defined
- email information is taken from config file to be used in functions
- an outputs folder is created if it doesn't already exist (refered to throughtout guidance as the publication folder, this is in a location specified in the config file, unless the process is run in test mode, then the outputs folder will remain in your repo)

Stages
1. Create the new release tables in SQL.
2. Export the new release SQL tables: Saved as txt files to a folder structure beginning SnomedCT_UKPrimaryCareRF2_[PRODUCTION/BETA]_YYYYMMDDT000000Z.
3. Update the SCT tables: Run a SQL query so UK SNOMED CT database tables are up to date.
4. Create GDPPR and GPData tables: produced in the UK SNOMED CT database, read into python and converted to CSVs, saved into output folder and zipped. Firearms spreadsheet populated. Emails created for people defined in config file.
5. Create Supporting Information Word Doc: Produces the UK NHS Primary Care data extraction reference sets supporting information document. 
6. Create a zip file: named ukpc.sct2_XX.0.0_YYYYMMDD, populated with the contents of SNOMEDCT_UKPrimaryCareRF2_PRODUCTION... folder (this folder being the highest level).
7. New TRUD Pack Request: create a change request form and generates an email containing the zipped file and change request form, Emails created for people defined in config file.
8. GDPPR/SPL cluster content export: C19 SNOMED changes file produced and attached to generated email to people defined in config file.
9. Query the ruleset published table to check it is up to date. Export a table with the results for checking.

Please Note:
This code has a built in test mode for when it is being run as a trial or dev work is being done. For the official release run, please make sure test mode is turned off. This is done through the cofig file: test = false.
"""

#log file created in cwd
log_loc, log_name = log_setup("Script2")

#read in config file and set test to True/False
config = get_config('./setup/config.toml')
test = config['Setup']['test_mode']
logging.info(f'Currently running the script in test mode: {test}.')

#find dates from config file and save into class 'Dates'
configDates = class_Dates(config['Dates'])

#replace dates in databases defined within config parameters
config = replace_db_dates_config_params(config, configDates)

#If in live mode, set databases classes (three separate) to contain live database/stored procedure names as properties
#If not in live mode, set database classes (three separate) to contain test database/stored procedure names as properties AND create/reset the test databases, as defined in the config, with the up to date records from the live databases.
UKSNOMEDCT, LocalSNOMEDCT, ClusterManagement = set_dbs_live_or_test(config,test)

#define filepaths from config
filepath_dict = filepaths(config['Filepaths'], configDates)
rel_ver00 = config['Filepaths']['rel_version'].lstrip('0') + '.0.0'

#try to find dates used in previous script(s)
configDates = find_previous_defined_dates(test, filepath_dict, configDates, UKSNOMEDCT, LocalSNOMEDCT)


#update the UKSNOMEDCT database class - now that Prev_monYY has been calculated using the intial current MMMYY database
config['LiveDatabases']['UKSNOMEDCT']['prior_db'] = config['LiveDatabases']['UKSNOMEDCT']['prior_db'].replace('Prev_MonYY',configDates.MmmYY_prev)
UKSNOMEDCT, LocalSNOMEDCT, ClusterManagement = set_dbs_live_or_test(config,test)


#define the email information from the config file and replace name
configEmails = replace_email_names_config_params(config)

#output fldr created in cwd
create_output_folder(filepath_dict)

#create the SQL tables to be exported
create_tables(configDates.PCDreleaseDate, LocalSNOMEDCT)

#export SQL tables to SNOMEDCT folder
export_tables(configDates=configDates, filepath_dict=filepath_dict, SnomedCT_Folders_Creation_vars=config['SnomedCT_Folders_Creation']
              , LocalSNOMEDCT=LocalSNOMEDCT)

#update SCT Tables
update_SCT_tables(configDates=configDates, test=test, UKSNOMEDCT=UKSNOMEDCT, LocalSNOMEDCT=LocalSNOMEDCT)

#produce supporting products
supportingProducts(configDates=configDates, filepath_dict=filepath_dict, email_dict=configEmails['Email_1'], config=config, UKSNOMEDCT=UKSNOMEDCT, LocalSNOMEDCT=LocalSNOMEDCT , ClusterManagement=ClusterManagement, Name=config['Setup']['full_name'], test=test)

#produce Supporting Information Word Document
supportingInfo(configDates=configDates, filepath_dict=filepath_dict, rel_version=rel_ver00, config_documents=config['Documents'], LocalSNOMEDCT=LocalSNOMEDCT)

#Zip SNOMEDCT folder
ukpcsct2_zip_upload(filepath_dict)

#produce TRUD Pack Request Word Document
TRUD_pack_req(configDates, filepath_dict, rel_version=rel_ver00, email_dict=configEmails['Email_2'], document_info=config['Documents'], Name=config['Setup']['full_name'])

#GDPPR / SPL cluster content changes
GDPPR_SPL_content(configDates=configDates, publishing_TRUD_fldr=filepath_dict['publishing_TRUD_fldr'], rel_version=rel_ver00, configEmails=configEmails, UKSNOMEDCT=UKSNOMEDCT, LocalSNOMEDCT=LocalSNOMEDCT, ClusterManagement=ClusterManagement)

#Check the Rulesets Published table is up to date
update_RP(ClusterManagement, configDates.PCDreleaseDate, output=filepath_dict['publishing_TRUD_fldr'])

#save dates used in this script
configDates.save_down_dates(filepath_dict["publishing_TRUD_fldr"])

#Run the full expanded cluster changes function, which outputs files and creates email to send to DGAT team
expanded_clusters_changes_full_funx(config, configDates, configEmails, filepath_dict, UKSNOMEDCT, LocalSNOMEDCT, ClusterManagement)

#move outputs to publication folder if in live mode, otherwise leave in cwd
filepath_dict = move_outputs(filepath_dict, script=2, test=test)

logging.info('Script 2 completed successfully. Please read through the log for any warning messages.')
logging.shutdown()

#move logger to publication folder if in live mode, otherwise leave in cwd
shutil.move(PurePath(log_loc, log_name), PurePath(filepath_dict['publishing_TRUD_fldr'], log_name))