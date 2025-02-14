#-------------------------------------------------------------------------------------------------------------------
# set up test tables in test database environment

def test_setup(test_db_conn, live_db_conn, test_tbl_name, live_tbl_name):

    query = f'''
                Drop table if exists {test_db_conn}.dbo.{test_tbl_name}
				Select * into {test_db_conn}.dbo.{test_tbl_name} from {live_db_conn}.dbo.{live_tbl_name}
             '''
    
    return query
#-------------------------------------------------------------------------------------------------------------------
