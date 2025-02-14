import logging
import os
import sys
import tomli
import shutil
import pandas as pd
from datetime import datetime
from pathlib import PurePath

from src.release_stages.Module_dependency import mod_dep_dates

#---------------------------------------------------------------------------------------------

def log_setup(Stage):
    """
    Function Actions:
    - Set up logging file with script name and timestamp.
    - Formats the log message as 'info'.
    - Saves into the cwd.
    """
    log_loc = os.getcwd() + '\\'

    #remove all current existing handlers
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
        
    #define time, log name and filepath with name for log
    timestamp = str(datetime.today()).replace(":",".")[:-10]
    log_name = f"{Stage}_PCD_Release_Log {timestamp}.log"
    filename = f"{log_loc}{log_name}"
    
    #setup logging configuration
    logging.basicConfig(#filename=filename
                        encoding='utf-8'
                        #, filemode='a'
                        , level=logging.DEBUG
                        , format='%(asctime)s %(levelname)s:%(name)s | %(message)s' #format of text in logger
                        , datefmt='%H:%M:%S'
                        , handlers=[
                            logging.FileHandler(filename)       #first handler to print log message log file
                            ,logging.StreamHandler(sys.stdout)  #second handler to print log message to screen 
                        ])
    
    #set up logger and write a message to it
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logging.info(f"{log_name} created")

    return log_loc, log_name

#---------------------------------------------------------------------------------------------

def get_config(loc:str) -> dict:
    """
    Function Actions:
    - Reads the config toml file containing user inputs.
    - Returns a dictionary of parameters.
    """   

    filename = PurePath(loc)
    assert os.path.isfile(filename), "config.toml file could not be found. Please make sure the toml file is saved in the main repo location and the name is 'config.toml' as expected."

    with open(filename, "rb") as f:
        config_file = tomli.load(f)

    print('Config file read in.')

    return config_file

#---------------------------------------------------------------------------------------------

def create_output_folder(filepath_dict:dict):
    """
    Function Actions:
    - If test equals false: creates a publication folder in //001_Publishing_TRUD_Release_SQL// if it does not already exist.
    - If test equals true: creates a test folder located in your cwd if one doesn't already exist.
    - Copies across all templates that require manual updates and SQL queries run outside of the python script.
    """

    #define filepaths from filepath dictionary
    output_fldr = filepath_dict['output_fldr']
    prework_fldr = filepath_dict['prework_fldr']
    publishing_TRUD_fldr = filepath_dict['publishing_TRUD_fldr']
    PowerBIxlsx_fldr = filepath_dict['PowerBIxlsx_fldr']
    PowerBItxt_fldr = filepath_dict['PowerBItxt_fldr']
    std_temp_fldr = filepath_dict['std_temp_fldr']
    sql_temp_fldr = filepath_dict['sql_temp_fldr']
    #sql_pub_fldr = PurePath(publishing_TRUD_fldr,'manual_SQL_queries')
    changed_ECC_ruleset_fldr = PurePath(publishing_TRUD_fldr,'Changed_Expanded_cluster_lists_Ruleset-level')
    changed_ECC_service_fldr = PurePath(publishing_TRUD_fldr,'Changed_ECC_Services')
    
    #create output folders
    for folder in [output_fldr, prework_fldr, publishing_TRUD_fldr, PowerBIxlsx_fldr, PowerBItxt_fldr, changed_ECC_ruleset_fldr, changed_ECC_service_fldr]:
        try: 
            os.mkdir(folder)
            logging.info(f'{folder} created.')
        except FileExistsError:
            logging.warning(f'{folder} not created as it already exists.')

    # #copy sql queries template folder to publication folder
    # try:
    #     shutil.copytree(src=sql_temp_fldr, dst=sql_pub_fldr) 
    #     logging.info('SQL_queries for manual running added to publication folder')
    #     # try:
    #     #     shutil.rmtree(sql_pub_fldr+'Archive\\')
    #     # except:
    #     #     print()
    # except FileExistsError:
    #     print('SQL_queries for manual running already in publication folder')

    #define any individual files in the main template folder that also need copying to publication folder:
    for file in ['Firearms_trigger_refset_membership_YYYYMMDD.xlsx']:
        shutil.copyfile(PurePath(std_temp_fldr, file), PurePath(publishing_TRUD_fldr, file))


#---------------------------------------------------------------------------------------------        

def move_outputs(filepath_dict:dict, script:int, test=bool):
    """
    Function Actions:
    - If running in live mode, move output folder to location in config file
    - Update filepath dictionary
    """

    assert script == 1 or script == '1' or  script == 2 or script == '2' or script == 3 or script == '3', "Script parameter assigned incorrectly to move_outputs function."

    #don't move files in test mode
    if test == True:
        logging.info('Outputs left in outputs folder in test mode.')
        return filepath_dict
    
    # if we're running main_script_1 or main_script_2:
    elif script == 1 or script == '1' or  script == 2 or script == '2':
        #if either filepath has been left empty, don't move files
        for path in [filepath_dict['rootPublicationOutput'], filepath_dict['rootSNOMED_CT']]:
            if '/' not in str(path) and "\\" not in str(path):
                logging.info('Output filepath empty in config file, outputs left in outputs folder.')
                return filepath_dict
            if os.path.isdir(path) == False:
                logging.warning(f'{str(path)} in config file is invalid. No outputs have been moved (Outputs have been left in the outputs folder).')
                return filepath_dict

        #move publication folder
        try:
            shutil.move(src=filepath_dict['publishing_TRUD_fldr'], dst=filepath_dict['rootPublicationOutput'])
            logging.info(f"Publication outputs moved to {str(filepath_dict['rootPublicationOutput'])}")
            filepath_dict['publishing_TRUD_fldr'] = PurePath(filepath_dict['rootPublicationOutput'], filepath_dict['publishing_TRUD_ext'])
        except shutil.Error:
            if os.path.isdir(PurePath(filepath_dict['rootPublicationOutput'], filepath_dict['publishing_TRUD_ext'])) == True:
                logging.warning(f"Publication output folder not moved to {str(filepath_dict['rootPublicationOutput'])} since a folder of the same name already exists in this location. If you wish to move it, please do this manually. It has been kept in the outputs folder.")
                for UID in os.listdir(filepath_dict['publishing_TRUD_fldr']):
                    try:
                        shutil.move(src=PurePath(filepath_dict['publishing_TRUD_fldr'], UID), dst=PurePath(filepath_dict['rootPublicationOutput'], filepath_dict['publishing_TRUD_ext']))
                        logging.info(f"{UID} moved to {str(PurePath(filepath_dict['rootPublicationOutput'], filepath_dict['publishing_TRUD_ext']))}")
                    except shutil.Error:
                        logging.info(f"{UID} already exists in {str(PurePath(filepath_dict['rootPublicationOutput'], filepath_dict['publishing_TRUD_ext']))}. This file has been left in the output folder so as not to overwrite any existing files.")
            else:
                logging.error('Error trying to move publication outputs folder. Please manually investigate.')
        
        # script 2 only
        if script == 2:
            #move SNOMED folder
            try:
                shutil.move(src=filepath_dict['SNOMEDCT_UK_fldr'], dst=filepath_dict['rootSNOMED_CT'])
                logging.info(f"SNOMEDCT output folder moved to {str(filepath_dict['rootSNOMED_CT'])}")
            except shutil.Error:
                if os.path.isdir(PurePath(filepath_dict['rootSNOMED_CT'], filepath_dict['SNOMEDCT_UK_ext'])) == True:
                    logging.warning(f"SNOMEDCT output folder not moved to {filepath_dict['rootSNOMED_CT']} since a folder of the same name already exists in this location. If you wish to move it, please do this manually. It has been kept in the outputs folder.")
                else:
                    logging.error('Error trying to move SNOMEDCT folder. Please manually investigate.')
    
    # if we're running main_script_3:
    elif script == 3 or script == '3':
        #if either filepath has been left empty, don't move files
        for path in [filepath_dict['rootPublicationOutput'], filepath_dict['rootPowerBIxlsx'], filepath_dict['rootPowerBItxt']]:
            if '/' not in str(path) and "\\" not in str(path):
                logging.info('Publication folder or PowerBI output filepath empty in config file, outputs left in outputs folder.')
                return filepath_dict
            if os.path.isdir(path) == False:
                logging.warning(f'{str(path)} in config file invalid. No outputs have been moved.')
                return filepath_dict
            
        #move PowerBI xlsx folder
        for UID in os.listdir(filepath_dict['PowerBIxlsx_fldr']):
            try:
                #overwrite file in PowerBIxlsx root folder from config file
                shutil.move(src=PurePath(filepath_dict['PowerBIxlsx_fldr'], UID), dst=PurePath(filepath_dict['rootPowerBIxlsx'], UID))
                logging.info(f"{UID} moved to {str(filepath_dict['rootPowerBIxlsx'])}")
                
            except shutil.Error:
                print(f"{UID} not moved to {str(filepath_dict['rootPowerBIxlsx'])}. This has been left in the outputs folder. Please manually investigate before refreshing the PowerBI.")

        #move PowerBI xlsx folder
        for UID in os.listdir(filepath_dict['PowerBItxt_fldr']):
            try:
                #overwrite file in PowerBIxlsx root folder from config file
                shutil.move(src=PurePath(filepath_dict['PowerBItxt_fldr'], UID), dst=PurePath(filepath_dict['rootPowerBItxt'], UID))
                logging.info(f"{UID} moved to {str(filepath_dict['rootPowerBItxt'])}")
                
            except shutil.Error:
                print(f"{UID} not moved to {str(filepath_dict['rootPowerBItxt'])}. This has been left in the outputs folder. Please manually investigate before refreshing the PowerBI.")
        
        #reset publishingTRUD folder to config file location
        filepath_dict['publishing_TRUD_fldr'] = PurePath(filepath_dict['rootPublicationOutput'], filepath_dict['publishing_TRUD_ext'])
        
    return filepath_dict


    #---------------------------------------------------------------------------------------------

def replace_email_names_config_params(config):
    #define the email information from the config file and replace name
    configEmails = config['Autogenerated_Emails']
    for email in configEmails:
        configEmails[email]['body'] = configEmails[email]['body'].replace("<Name>", config['Setup']['full_name'])

    return  configEmails


#----------------------------------------------------------------------------
def find_previous_defined_dates(test, filepath_dict, configDates, UKSNOMEDCT, LocalSNOMEDCT):    
    
    #try to find dates used in previous script(s). If done so an excel file has saved down each of these date parameters.
    try: 
        if test == False:
            fp = PurePath(filepath_dict['rootPublicationOutput'], "date_variables.xlsx")
        else:
            fp = PurePath(filepath_dict['publishing_TRUD_fldr'], "date_variables.xlsx")
        
        dates = pd.read_excel(fp, index_col=0)
        logging.info(f"Additional dates found from {fp}")
        configDates.dates_from_file(dates)

    except FileNotFoundError: #dates not saved down/previous script not run. Find dates from sql/user input
    #find if our monolith snapshot already exists and pull through remaining dates from the table
    #otherwise define additional dates through user input in the terminal
        MD_pd_actual_data = mod_dep_dates(configDates, UKSNOMEDCT, LocalSNOMEDCT)
        #add these dates to the configDates class
        configDates.prev_MmmYY(UKSNOMEDCT)
        configDates.INT_Drug_Path(MD_pd_actual_data)   

    return configDates 