import pytest
import pandas as pd
import sqlalchemy as sa
#from sqlalchemy.sql import text

from pathlib import PurePath

from src.utils.connection_fxns import sa_connect, get_df_from_sql, execute_sql
from src.utils.setup_fxns import get_config

test_config = get_config('./tests/test_config.toml')
test_config = test_config['unittests']['connection']

valid_svr = test_config['svr']['valid']
invalid_svr = test_config['svr']['invalid']

valid_db = test_config['db']['valid']
invalid_db = test_config['db']['invalid']

valid_tbl = test_config['tbl']['valid']
invalid_tbl = test_config['tbl']['invalid']

# test sa_connect function creates connection
def test_sa_connection():
    assert type(sa_connect(server=valid_svr, database=valid_db)) == sa.future.engine.Engine, "Connection error on test"

# test sa_connect function errors with incorrect server name
def test_sa_connection_error_check1():
    with pytest.raises(sa.exc.OperationalError):
        engine = sa_connect(server=invalid_svr, database=valid_db)
        engine.connect()
        
# test sa_connect function errors with incorrect db name
def test_sa_connection_error_check2():
    with pytest.raises(sa.exc.ProgrammingError):
        engine = sa_connect(server=valid_svr, database=invalid_db)
        engine.connect()

# test get_df_from_sql function returns a df
def test_return_df():
    server=valid_svr
    database=valid_db

    query = f'''SELECT TOP (100) * 
                FROM [dbo].[{valid_tbl}]'''
    
    df = get_df_from_sql(server, database, query)

    assert type(df) == pd.DataFrame, "Connection/SQL error on returning df test"

# test get_df_from_sql function errors with incorrect sql code
def test_return_df_error_check1():
    server=valid_svr
    database=valid_db

    query = f'''SELECT TOP (100) * 
                FROM [dbo].[{invalid_tbl}]'''
    
    with pytest.raises(sa.exc.ProgrammingError):
        df = get_df_from_sql(server, database, query)

# test get_df_from_sql function errors with incorrect db name
def test_return_df_error_check2():
    server=valid_svr
    database=invalid_db

    query = f'''SELECT TOP (100) * 
                FROM [dbo].[{valid_tbl}]'''
    
    with pytest.raises(sa.exc.ProgrammingError):
        df = get_df_from_sql(server, database, query)

# test execute_sql function
def test_execute_sql():
    server=valid_svr
    database=valid_db

    query = f'''SELECT TOP (100) * 
                into #TT1
                FROM [dbo].[{valid_tbl}]'''
    
    execute_sql(server, database, query)

# test execute_sql function errors with incorrect db name
def test_execute_sql_error_check1():
    server=valid_db
    database=invalid_db

    query = f'''SELECT TOP (100) * 
                into #TT1
                FROM [dbo].[{valid_tbl}]'''
    
    with pytest.raises(sa.exc.OperationalError):
        execute_sql(server, database, query)

# test execute_sql function errors with incorrect tbl name
def test_execute_sql_error_check2():
    
    query = f'''SELECT TOP (100) * 
                into #TT1
                FROM [dbo].[{invalid_tbl}]'''
    
    with pytest.raises(sa.exc.OperationalError):
        execute_sql(server=valid_db, database=valid_db, query=query)

# test execute_sql function errors with incorrect sql code - from/into wrong lines
def test_execute_sql_error_check3():

    query = f'''SELECT TOP (100) * 
                FROM [dbo].[{valid_tbl}]
                into #TT1'''
    
    with pytest.raises(sa.exc.OperationalError):
        execute_sql(server=valid_db, database=valid_db, query=query)
