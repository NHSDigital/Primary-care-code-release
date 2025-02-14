import logging
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.sql import text

#---------------------------------------------------------------------------------------------
def sa_connect(server:str, database:str) -> sa.engine:
    """
    Create a SQL Alchemy connection and return the engine.
    """
    connection_string = "DRIVER={SQL Server};SERVER=%(svr)s;DATABASE=%(db)s;Trusted_Connection=yes" % {'db':database, 'svr':server}
    connection_url = sa.engine.URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
    engine = sa.create_engine(connection_url,future=True)

    return engine

#---------------------------------------------------------------------------------------------
def get_df_from_sql(server:str, database:str, query:str) -> pd.DataFrame:
    """
    Create a SQL Alchemy connection to server-database and execute query. Commit any changes. Return all results as a pandas dataframe.
    """
	#define the sqlalchemy connection
    engine = sa_connect(server, database)

    with engine.connect() as conn:
        #run the query and return it as dataframe
        query = conn.execute(text(query))
        df = pd.DataFrame(query.fetchall())
        #commit any changes made in the query to the server-database
        conn.commit()

    return df
#---------------------------------------------------------------------------------------------
def execute_sql(server:str, database:str, query:str):
    """
    Create a SQL Alchemy connection to server-database and execute query. Commit any changes.
    """
	#define the sqlalchemy connection
    engine = sa_connect(server, database)

    with engine.connect() as conn:
        #run the query
        result = conn.execute(text(query))         
        conn.commit()

#---------------------------------------------------------------------------------------------