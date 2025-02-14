import shutil
import logging
from pathlib import PurePath
from src.utils.dates import class_Dates
from src.utils.filepaths import filepaths
from src.utils.databases import set_dbs_live_or_test, replace_db_dates_config_params
from src.utils.setup_fxns import log_setup, get_config, create_output_folder, move_outputs
#from src.release_stages.bulk_find_replace import bulk_find_replace
from src.release_stages.Module_dependency import mod_dep_dates, Monolith_replacements
from src.release_stages.TRUD_table_Cleardown import cleardown_TRUD_tables

"""
Script 1 Actions:

Setup
- a logging file is created for run outputs and times, and any warnings or errors will be flagged here
- user inputs are read in from the config file
- additional dates and filepaths are defined
- an outputs folder is created if it doesn't already exist (refered to throughout guidance as the publication folder, this is in a location specified in the config file, unless the process is run in test mode, then the outputs folder will remain in your repo)

Stages 

1. Template sql queries: Copied into the publication folder and a bulk find and replace of all dates and SCT_ prefix occurs. These queries are not used in the python scripts but need to be manually run between script 1 & 2.
2. Module dependency monolith replacements: Adds in each of the module dependencies to the Module Dependency table (in the Local UK SNOMED CT database), where the release is dependent on other external releases.
3. Remove historical versions of tables from the Local UK SNOMED CT database: keep only one version (tables with current date will be produced to total two versions).

Please Note: 
This code has a built in test mode for when it is being run as a trial or dev work is being done. For the official release run, please make sure test mode is turned off. This is done through the cofig file: test = false.
"""

#log file created in cwd
log_loc, log_name = log_setup('Script1')

#read in config file and set test to True/False
config = get_config('./setup/config.toml')
test = config['Setup']['test_mode']
logging.info(f'Currently running the script in test mode: {test}.')

#find dates from config file and save into class 'Dates' 
configDates = class_Dates(config['Dates'])

#replace dates in databases defined within config parameters
config = replace_db_dates_config_params(config, configDates)

#if in live mode, set database classes (three separate) to contain live database/stored procedure names as properties 
#if not in live mode, set database classes (three separate) to contain test database/stored procedure names as properties AND create/reset the test databases, as defined in the config, with the up to date records from the live databases.
UKSNOMEDCT, LocalSNOMEDCT, ClusterManagement = set_dbs_live_or_test(config,test)

#find if our monolith snapshot already exists and pull through remaining dates from the table
#otherwise define additional dates through user input in the terminal
MD_pd_actual_data = mod_dep_dates(configDates, UKSNOMEDCT=UKSNOMEDCT, LocalSNOMEDCT=LocalSNOMEDCT)
#add these dates to the configDates class
configDates.prev_MmmYY(UKSNOMEDCT)
configDates.INT_Drug_Path(MD_pd_actual_data)

#define filepaths from config
filepath_dict = filepaths(config['Filepaths'], configDates)

#output fldr created in cwd
create_output_folder(filepath_dict=filepath_dict)

##### #bulk find and replace
##### bulk_find_replace(configDates, sql_pub_fldr=filepath_dict['sql_pub_fldr'])   

#monolith replacements (module dependency)
Monolith_replacements(MD_pd_actual_data, configDates, LocalSNOMEDCT)

#clear down Local UK SNOMED CT database
cleardown_TRUD_tables(LocalSNOMEDCT)

#save dates used in this script
configDates.save_down_dates(filepath_dict["publishing_TRUD_fldr"])

#move outputs to publication folder if in live mode, otherwise leave in cwd
filepath_dict = move_outputs(filepath_dict, script=1, test=test)

logging.info('Script 1 completed successfully. Please read through the log for any warning messages.')
logging.shutdown()

#move logger to publication folder if in live mode, otherwise leave in cwd
shutil.move(PurePath(log_loc, log_name), PurePath(filepath_dict['publishing_TRUD_fldr'], log_name))
