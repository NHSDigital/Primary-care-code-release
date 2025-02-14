import logging
import pandas as pd
from pathlib import PurePath
from pathlib import Path
import shutil

from src.utils.dates import class_Dates
from src.utils.filepaths import filepaths
from src.utils.databases import set_dbs_live_or_test, replace_db_dates_config_params 
from src.utils.setup_fxns import log_setup, get_config, replace_email_names_config_params, find_previous_defined_dates, create_output_folder
from src.release_stages.Module_dependency import mod_dep_dates
from src.release_stages.expanded_clusters_changes import expanded_clusters_changes_full_funx


#log file created in cwd
log_loc, log_name = log_setup("ECC_only_run")

#read in config file and set test to True/False
config = get_config('./setup/config.toml')
test = config['Setup']['test_mode']

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

#define the email information from the config file and replace name
configEmails = replace_email_names_config_params(config)

#output fldr created in cwd
create_output_folder(filepath_dict)

#Run the full expanded cluster changes function, which outputs files and creates email to send to  DGAT team
expanded_clusters_changes_full_funx(config, configDates, configEmails, filepath_dict, UKSNOMEDCT, LocalSNOMEDCT, ClusterManagement)

#close out and move logger file
logging.info('ECC_only_run completed successfully. Please read through the log for any warning messages.')
logging.shutdown()

#move logger to publication folder if in live mode, otherwise leave in cwd
shutil.move(PurePath(log_loc, log_name), PurePath(filepath_dict['publishing_TRUD_fldr'], log_name))