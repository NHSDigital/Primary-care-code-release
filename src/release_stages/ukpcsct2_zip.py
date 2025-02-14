#stage 9 function
import shutil
import logging
from pathlib import PurePath

def ukpcsct2_zip_upload(filepath_dict:dict):
    """
    Function Actions:
    - Creates a new zip file called ukpc.sct2_XX.0.0_YYYYMMDD
    - Populates it with the contents of the SNOMEDCT_UKPrimaryCareRF2_PRODUCTION... folder (this folder being the highest level).
    """
    #define dates from dates dictionary
    SNOMEDCT_UK_ext = filepath_dict['SNOMEDCT_UK_ext']
    uk_sct2pc_ext = filepath_dict['uk_sct2pc_ext']
    output_fldr = filepath_dict['output_fldr']

    logging.info("Creating ukpcsct2 zip...")

    #create a zipfile of folder above (with structure)
    #named: base_dir (test folder in test mode, else root TRUD folder)
    #starting point for folder structure:root_dir
    #point for entire contents below zipped:base_dir - always exclude entire folder path, just name below root_dir
    shutil.make_archive(base_name=PurePath(output_fldr,uk_sct2pc_ext), format='zip', root_dir=output_fldr, base_dir=SNOMEDCT_UK_ext)
    logging.info(uk_sct2pc_ext + ' zip archive created.')