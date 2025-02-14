"""
To run this test, you will need to move the script into the main folder (pcd-snomed-release)
This can then be run from the terminal: python test_filepaths.py
"""

import pytest

from src.utils.setup_fxns import get_config
from src.utils.filepaths import *

config = get_config('./tests/test_config.toml')
config = config['unittests']['filepath']

configDates = class_Dates({'MmmYY':'Apr23'
              ,'UKreleaseDate':'20230412'
              ,'PrevUKrelDate':'20230412'
              ,'PCDreleaseDate':'20230718'
              ,'PrevPCDrelDate':'20230518'
              })

def test_filepaths():
  # MAKE THIS CORRECT - this is using correct input - we don't want this to fail!
  configPaths = {'SNOMED_CT': config['SNOMED_CT']['valid']
                  ,'Publication_Outputs': config['Publication_Outputs']['valid']
                  ,'rel_version': config['rel_version']['valid']
                  }
  
  configPaths_empty = {'SNOMED_CT': ''
                  ,'Publication_Outputs': ''
                  ,'rel_version': config['rel_version']['valid']
                  }
  
  for paths in [configPaths, configPaths_empty]:
    filepath_dict = filepaths(paths, configDates)
    assert type(filepath_dict) == dict

# missing a key:value pair
def test_filepaths_error_check1():
  # missing variable in dictionary
  configPaths1 = {'SNOMED_CT': config['SNOMED_CT']['valid']
                  ,'Publication_Outputs': config['Publication_Outputs']['valid']
                  }
  
  with pytest.raises(KeyError):
    filepath_dict = filepaths(configPaths1, configDates)
        

def test_filepaths_error_check2():
  # rel version int not str
  configPaths2 = {'SNOMED_CT': config['SNOMED_CT']['valid']
                  ,'Publication_Outputs': config['Publication_Outputs']['valid']
                  ,'rel_version':44
                  }
  # rel version wrong length
  configPaths3 = {'SNOMED_CT': config['SNOMED_CT']['valid']
                  ,'Publication_Outputs': config['Publication_Outputs']['valid']
                  ,'rel_version':'44'
                  }
  
  for paths in [configPaths2, configPaths3]:
    with pytest.raises(AssertionError):
      filepath_dict = filepaths(paths, configDates)