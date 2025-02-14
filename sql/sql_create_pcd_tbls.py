#---------------------------------------------------------------------    
def sql_query_replace_9999(YYYYMMDD,tbl,col):

    '''
    Inputs:
    YYYYMMDD = the date of the planned PCD release.
    tbl = a maintained pcd table name
    col = column name - this is expected to be an 'effectiveTime' column

    Actions:
    This updates any 99999999 values to the planned PCD release date in the specified effectivetime column in the specified table.
    '''

    MySQL = f"""
        UPDATE {tbl}					
        SET {col} = '{YYYYMMDD}'
        where {col} = '99999999'
    """
    return MySQL
#---------------------------------------------------------------------
def sql_query_make_full_snap_delta(original, full_tbl, YYYYMMDD, db):

    '''
    Inputs:
    original = maintained (full ongoing) table (varies dependent on test mode) - where we will get the data from
    full_tbl = the 'full' table naming structure for the 'original' table - this is what will be created.
    YYYYMMDD = the date of the planned PCD release.
    db = the database containing 'original'

    Actions:
    This query creates 3 tables in the specified database which are full, snapshot and delta format versions of the 'original' table's data.
    These tables will be renamed and exported into the release pack later in the process.
    '''

    snap_tbl = full_tbl.replace('Full','Snapshot')
    delta_tbl = full_tbl.replace('Full','Delta')

    MySQL = f"""
                USE {db}

                --Full
                DROP tABLE if exists dbo.{full_tbl}
                Select * into dbo.{full_tbl} from {original}

                --delta
                DROP tABLE if exists dbo.{delta_tbl}
                Select * into dbo.{delta_tbl} from {original}
                where effectiveTime = '{YYYYMMDD}'

                --Snapshot
                DROP tABLE if exists dbo.{snap_tbl}
                Select distinct b.* into dbo.{snap_tbl} 
        		From (	Select distinct id, max(effectiveTime) as [effectiveTime]
				        from {original}
        				group by id) a
		        inner join {original} b
		        on a.id = b.id AND a.effectiveTime = b.effectiveTime
            """
    return MySQL
    #------------------------------------------------------------------------