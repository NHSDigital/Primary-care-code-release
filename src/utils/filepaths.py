import logging
import os
from pathlib import PurePath
from src.utils.dates import class_Dates

def filepaths(configPaths:dict, configDates:class_Dates):

    """
    Function Actions:
    - Returns a dictionary of all filepaths used throughout the python scripts. 
    - If the script is being run in test mode, the test folder will be used as the root and publication filepaths instead.
    - filepath functions that start with "root_" mean that original files will be saved in the shared drive location.
    """

    #define the variables from the config file
    try:
        rel_version = configPaths['rel_version']
    except KeyError:
        logging.critical('Release version (rel_version) has not been defined in config file. Please amend config file.')
        raise KeyError('Release version (rel_version) has not been defined in config file. Please amend config file.')
    
    assert type(rel_version) == str, f"Release version (rel_version) given was type {type(rel_version)}, but was expecting a string. Please amend config file."
    assert len(rel_version) == 3, f"Release version (rel_version) is in incorrect format. Expected '000'. Please amend config file."
    
    try:
        rootSNOMED_CT = PurePath(configPaths['SNOMED_CT'])
    except KeyError:
        logging.warning('SNOMED_CT has not been defined in config file. This has been set to empty.')
        rootSNOMED_CT = ''
    
    try:
        rootPublicationOutput = PurePath(configPaths['Publication_Outputs'])
    except KeyError:
        logging.warning('Publication_Outputs has not been defined in config file. This has been set to empty.')
        rootPublicationOutput = ''

    try:
        rootPowerBI_xlsx = PurePath(configPaths['PowerBI_xlsx_Outputs'])
    except KeyError:
        logging.warning('PowerBI_xlsx_Outputs has not been defined in config file. This has been set to empty.')
        rootPowerBI_xlsx = ''
    
    try:
        rootPowerBI_txt = PurePath(configPaths['PowerBI_txt_Outputs'])
    except KeyError:
        logging.warning('PowerBI_txt_Outputs has not been defined in config file. This has been set to empty.')
        rootPowerBI_txt = ''

    #define location of templates and inputs folder in repo, and manual SQL queries folder
    std_temp_fldr = PurePath(os.getcwd(), 'templates_and_inputs')
    sql_temp_fldr = PurePath(std_temp_fldr, 'manual_SQL_queries')

    #define location of outputs folder in repo
    output_fldr = PurePath(os.getcwd(), 'outputs')

    #define folder extension variables that need to remain the same for test mode and actual running
    SNOMEDCT_UK_ext = f"SnomedCT_UKPrimaryCareRF2_PRODUCTION_{configDates.YYYYMMDD(configDates.PCDreleaseDate)}T000000Z"
    uk_sct2pc_ext = f"uk_sct2pc_{rel_version.lstrip('0')}.0.0_{configDates.YYYYMMDD(configDates.PCDreleaseDate)}000000Z"
    publishing_TRUD_ext = f"{rel_version}_{configDates.ddmmyy(configDates.PCDreleaseDate)}"
    
    #define folder locations
    SNOMEDCT_UK_fldr = PurePath(output_fldr, SNOMEDCT_UK_ext)
    publishing_TRUD_fldr = PurePath(output_fldr, publishing_TRUD_ext)
    ukpc_sct2_fldr = PurePath(output_fldr, uk_sct2pc_ext)
    prework_fldr = PurePath(output_fldr,'PreWork')
   
    SupportingProducts_fldr = PurePath(SNOMEDCT_UK_fldr, "SupportingProducts")
    Documentation_fldr = PurePath(SNOMEDCT_UK_fldr, "Documentation")

    #sql_pub_fldr = PurePath(publishing_TRUD_fldr,'manual_SQL_queries')
    changed_ECC_ruleset_fldr = PurePath(publishing_TRUD_fldr,'Changed_Expanded_cluster_lists_Ruleset-level')
    changed_ECC_service_fldr = PurePath(publishing_TRUD_fldr,'Changed_ECC_Services')

    PowerBIxlsx_fldr = PurePath(output_fldr, "002_Tables_for_Power_BI_report")
    PowerBItxt_fldr = PurePath(output_fldr, "004_txt_files")

    #create a dictionary with all folder extensions/folder locations to return
    filepath_dict = {
        'rootSNOMED_CT'            : rootSNOMED_CT
        ,'rootPublicationOutput'    : rootPublicationOutput
        ,'rootPowerBIxlsx'          : rootPowerBI_xlsx
        ,'rootPowerBItxt'           : rootPowerBI_txt
        ,'output_fldr'              : output_fldr
        ,'SNOMEDCT_UK_ext'          : SNOMEDCT_UK_ext
        ,'uk_sct2pc_ext'            : uk_sct2pc_ext
        ,'publishing_TRUD_ext'      : publishing_TRUD_ext
        ,'std_temp_fldr'            : std_temp_fldr
        ,'sql_temp_fldr'            : sql_temp_fldr
        #,'sql_pub_fldr'             : sql_pub_fldr
        ,'prework_fldr'             : prework_fldr
        ,'publishing_TRUD_fldr'     : publishing_TRUD_fldr
        ,'SNOMEDCT_UK_fldr'         : SNOMEDCT_UK_fldr
        ,'uk_sct2pc_fldr'           : ukpc_sct2_fldr
        ,'SupportingProducts_fldr'  : SupportingProducts_fldr
        ,'Documentation_fldr'       : Documentation_fldr
        ,'PowerBIxlsx_fldr'         : PowerBIxlsx_fldr
        ,'PowerBItxt_fldr'          : PowerBItxt_fldr
        ,'changed_ECC_ruleset_fldr'         : changed_ECC_ruleset_fldr
        ,'changed_ECC_service_fldr'         : changed_ECC_service_fldr
    }

    return filepath_dict