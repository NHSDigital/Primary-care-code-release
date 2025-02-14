"""
To run this test, you will need to move the script into the main folder (pcd-snomed-release)
This can then be run from the terminal: python test_setup.py
"""

import pytest
from pathlib import PurePath
import shutil

from src.utils.setup_fxns import create_output_folder, move_outputs
from src.utils.filepaths import *

#log_setup: nothing to test
#get_config: nothing to test through unit testing: file would have to be moved
#create_output_folder: check valid filepaths

# MAKE THIS CORRECT - this is using correct input - we don't want this to fail!
filepath_dict = {
                    'rootSNOMED_CT'         : PurePath('./tests/unittests/unittest_folder_to_move/'),
                    'output_fldr'           : PurePath('./outputs'), 
                    'SNOMEDCT_UK_ext'       : 'SnomedCT_UKPrimaryCareRF2_PRODUCTION_20230718T000000Z',
                    'uk_sct2pc_ext'         : 'ukpc_sct2_99.0.0_20230718',
                    'publishing_TRUD_ext'   : '099_18.07.23', 
                    'std_temp_fldr'         : PurePath('./templates_and_inputs'),
                    'sql_temp_fldr'         : PurePath('./templates_and_inputs/manual_SQL_queries'),
                    #'sql_pub_fldr'          : PurePath('./outputs/099_18.07.23/manual_SQL_queries'),
                    'publishing_TRUD_fldr'  : PurePath('./outputs/099_18.07.23'),
                    'SNOMEDCT_UK_fldr'      : PurePath('./outputs/SnomedCT_UKPrimaryCareRF2_PRODUCTION_20230718T000000Z'), 
                    'ukpc_sct2_fldr'        : PurePath('./outputs/ukpc_sct2_99.0.0_20230718'), 
                    'SupportingProducts_fldr': PurePath('./outputs/SnomedCT_UKPrimaryCareRF2_PRODUCTION_20230718T000000Z/SupportingProducts'),
                    'Documentation_fldr'    : PurePath('./outputs/SnomedCT_UKPrimaryCareRF2_PRODUCTION_20230718T000000Z/Documentation'),
                    'PowerBIxlsx_fldr'      : PurePath('./outputs/002_Tables_for_Power_BI_report'),
                    'PowerBItxt_fldr'       : PurePath('./outputs/004_txt_files')
                    }

def test_create_output_folder():
    create_output_folder(filepath_dict)
    assert os.path.exists(filepath_dict['output_fldr']), "Output folder doesn't exist - check function"

def test_move_outputs(): 
    move_outputs(filepath_dict, script=2, test=True)
    #move test folder back to original spot to be removed...
    shutil.move(src=filepath_dict['SNOMEDCT_UK_fldr'], dst=filepath_dict['rootSNOMED_CT'])

def test_move_outputs_error_check():
    with pytest.raises(AssertionError):
        move_outputs(filepath_dict, script="24", test=True)
