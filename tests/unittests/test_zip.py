import pytest

import os
import pandas as pd
from src.utils.file_fxns import zip_files

content = [{'filepath':'./tests/unittests/', 
                'name':'df1.csv'},
           {'filepath':'./tests/unittests/', 
                'name':'df1.csv'}]
zip_loc = {'filepath':os.getcwd() + '/outputs/', 
        'name':'example_zip.zip'}

def test_zip_files():     
    # shouldn't error
    zip_files(content_filepath_name=content, zf_filepath_name=zip_loc)
    assert os.path.exists(os.getcwd() + '/outputs/example_zip.zip')
