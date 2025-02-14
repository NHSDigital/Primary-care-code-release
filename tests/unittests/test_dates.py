"""
To run this test, you will need to move the script into the main folder (pcd-snomed-release)
This can then be run from the terminal: python test_dates.py
"""

import pytest
from src.utils.dates import *
from src.utils.databases import class_UKSNOMEDCT_dbs
from src.utils.setup_fxns import get_config

# test dates class is initiated correctly
def dates_():
    # MAKE THIS CORRECT - this is using correct input - we don't want this to fail!
    dates_dict = {'MmmYY':'Apr23'
                ,'UKreleaseDate':'20230412'
                ,'PrevUKrelDate': '20230412'
                ,'PCDreleaseDate':'20230718'
                ,'PrevPCDrelDate':'20230518'
                }
    
    dates = Dates(dates_dict)
    
    assert dates.MmmYY
    assert dates.UKreleaseDate
    assert dates.PrevUKrelDate
    assert dates.PCDreleaseDate
    assert dates.PrevPCDrelDate
    assert dates.INTreleaseDate == None
    assert dates.UKdrugsDate == None
    assert dates.PathologyDate == None
    assert dates.MmmYY_prev == None
    
    return dates

# missing key in passed dictionary    
def test_dates_error_check1():
    
    dates_dict = {'UKreleaseDate':'20230412'
                  ,'PrevUKrelDate':'20230412'
                 ,'PCDreleaseDate':'20230718'
                 ,'PrevPCDrelDate':'20230518'
                }
    
    with pytest.raises(KeyError):
        dates = Dates(dates_dict)

# incorrect MmmYY format in dictionary
def test_dates_error_check2():
    
    dates_dict = {'MmmYY':'Apr2023'
                ,'UKreleaseDate':'20230412'
                ,'PrevUKrelDate':'20230412'
                ,'PCDreleaseDate':'20230718'
                ,'PrevPCDrelDate':'20230518'
                }
    
    with pytest.raises(AssertionError):
        dates = Dates(dates_dict)

# incorrect YYYYMMDD format in dictionary
def test_dates_error_check3():
    
    dates_dict = {'MmmYY':'Apr23'
                ,'UKreleaseDate':'20230412'
                ,'PrevUKrelDate':'20230412'
                ,'PCDreleaseDate':'202307180'
                ,'PrevPCDrelDate':'20230518'
                }
    
    with pytest.raises(AssertionError):
        dates = Dates(dates_dict)

# incorrect int instead of str in dictionary
def test_dates_error_check4():
    
    dates_dict = {'MmmYY':'Apr23'
                ,'UKreleaseDate':'20230412'
                ,'PrevUKrelDate':'20230412'
                ,'PCDreleaseDate':20230718
                ,'PrevPCDrelDate':'20230518'
                }
    
    with pytest.raises(AssertionError):
        dates = Dates(dates_dict)

def test_dates_ddMonthYYYY_error_checks():
    dates = dates_()

    # int instead of str
    # too long
    params = [20230412, '202304120']
    for param in params:
        with pytest.raises(AssertionError):
            dates.ddMonthYYYY(param)
    
    # not a date
    with pytest.raises(ValueError):
        dates.ddMonthYYYY('20231412')

def test_dates_YYYYMMDD_error_checks():
    dates = dates_()

    # int instead of str
    # too long
    params = [20230412, '202304120']
    for param in params:
        with pytest.raises(AssertionError):
            dates.YYYYMMDD(param)
    
    # not a date
    with pytest.raises(ValueError):
        dates.YYYYMMDD('20231412')

def test_dates_YYYY_error_checks():
    dates = dates_()

    # int instead of str
    # too long
    params = [20230412, '202304120']
    for param in params:
        with pytest.raises(AssertionError):
            dates.YYYY(param)
    
    # not a date
    with pytest.raises(ValueError):
        dates.YYYY('20231412')

def test_dates_MonYr_error_checks():
    dates = dates_()

    # int instead of str
    # too long
    params = [20230412, '202304120']
    for param in params:
        with pytest.raises(AssertionError):
            dates.MonYY(param)
    
    # not a date
    with pytest.raises(ValueError):
        dates.MonYY('20231412')

def test_dates_ddmmyy_error_checks():
    dates = dates_()

    # int instead of str
    # too long
    params = [20230412, '202304120']
    for param in params:
        with pytest.raises(AssertionError):
            dates.ddmmyy(param)
    
    # not a date
    with pytest.raises(ValueError):
        dates.ddmmyy('20231412')

def test_dates_YmdDashes_error_checks():
    dates = dates_()

    # int instead of str
    # too long
    params = [20230412, '202304120']
    for param in params:
        with pytest.raises(AssertionError):
            dates.YmdDashes(param)
    
    # not a date
    with pytest.raises(ValueError):
        dates.YmdDashes('20231412')

def test_dates_prevMmmYY_error_check():
    dates = dates_()
    
    test_config = get_config('./tests/test_config.toml')
    test_config = test_config['unittests']

    valid_svr = test_config['connection']['svr']['valid']
    
    # database doesn't exist
    dates.MmmYY = test_config['MmmYY']

    UKSNOMEDCT = class_UKSNOMEDCT_dbs(SQLserver=valid_svr, dbUKSNOMEDCT=test_config['UKSNOMEDCT'], configIA0={'IA_SCT_Chosen':True})

    with pytest.raises(AssertionError):
        dates.prev_MmmYY(UKSNOMEDCT)
