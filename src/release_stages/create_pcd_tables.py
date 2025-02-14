import pandas as pd
import numpy as np
import logging
from src.utils.connection_fxns import execute_sql
from src.utils.databases import class_LocalSNOMEDCT_dbs
from sql.sql_create_pcd_tbls import sql_query_replace_9999, sql_query_make_full_snap_delta

#---------------------------------------------------------------------
def create_tables(YYYYMMDD, LocalSNOMEDCT:class_LocalSNOMEDCT_dbs):

    '''
    Inputs:
    YYYYMMDD is the date of the PCD release about to be published. 
    local_db = the local SNOMED database (where the PCD release is maintained in full) as specified in the config file (LocalSNOMEDCT.db).
    
    Acions:
    Makes a dataframe of maintained tables and a basic naming format needed for publication.
    Replaces all effectiveTime values of 99999999 (placeholder) with the date of the PCD release in the maintained tables.
    Creates each table in full, delta and snapshot format using the naming conventions in the dataframe.

    This should be run from main script by adding:
        MyServer = config['Connections']['SQL_Server']
        MyDate = configDates.PCDreleaseDate
        create_tables(MyDate, LocalSNOMEDCT, MyServer)
    '''

    db = LocalSNOMEDCT.db
    pc_server = LocalSNOMEDCT.server
    
    #list existing tables to be dated and saved as full, snapshot and delta tables (these table names will vary depending on whether we are in live or test mode)
    full_tbl_list = [LocalSNOMEDCT.Concept
                     ,LocalSNOMEDCT.Description
                     ,LocalSNOMEDCT.Relationship
                     ,LocalSNOMEDCT.Refsets                    
                     ,LocalSNOMEDCT.OwlExpression
                     ,LocalSNOMEDCT.Language
                     ,LocalSNOMEDCT.RefsetDescriptor
                     ,LocalSNOMEDCT.ModuleDependency
                     ]
    #list the structure of the table names they should be exported with 
    created_tbl_list =  ['[sct2_Concept_Full_1000230_'+YYYYMMDD+']'
                        ,'[sct2_Description_Full-en_1000230_'+YYYYMMDD+']'
                        ,'[sct2_Relationship_Full_1000230_'+YYYYMMDD+']'
                        ,'[der2_Refset_SimpleFull_1000230_'+YYYYMMDD+']'
                        ,'[sct2_sRefset_OWLExpressionFull_1000230_'+YYYYMMDD+']'
                        ,'[der2_cRefset_LanguageFull-en_1000230_'+YYYYMMDD+']'
                        ,'[der2_cciRefset_RefsetDescriptorFull_1000230_'+YYYYMMDD+']'
                        ,'[der2_ssRefset_ModuleDependencyFull_1000230_'+YYYYMMDD+']'
                        ]
          
    #For each table in the above full_tbl list, #execute SQL query to replace 99999999 in effectimeTime column with the date of the upcoming pcd release
    for t in full_tbl_list:
        execute_sql(server=pc_server, database=db, query=sql_query_replace_9999(YYYYMMDD,t,'effectiveTime'))
        logging.info(f'99999999 effectiveTime changed to {YYYYMMDD} in {t} table.')

    # Replace 2 extra dates in module dependency table 
    execute_sql(server=pc_server, database=db, query=sql_query_replace_9999(YYYYMMDD,LocalSNOMEDCT.ModuleDependency,'sourceEffectiveTime') )
    logging.info(f'99999999 sourceEffectiveTime changed to {YYYYMMDD} in {LocalSNOMEDCT.ModuleDependency}.')   
    
    execute_sql(server=pc_server, database=db, query=sql_query_replace_9999(YYYYMMDD,LocalSNOMEDCT.ModuleDependency,'targetEffectiveTime'))
    logging.info(f'99999999 targetEffectiveTime changed to {YYYYMMDD} in {LocalSNOMEDCT.ModuleDependency}.')   


    #make dataframe from the 2 lists of tables above
    tbl_lists_into_dict =  {'original_table':full_tbl_list
                       ,'name_structure':created_tbl_list}
    tbl_name_df = pd.DataFrame(data=tbl_lists_into_dict)


    #Loop through dataframe - for each original (maintained) table, save copies as full snapshot and delta with chosen naming convention (name_structure)
    for i in tbl_name_df.index:
        
        #Pull out the 2 values needed
        original = tbl_name_df.loc[i,'original_table']  # table to be copied.
        naming =  tbl_name_df.loc[i,'name_structure']   # the full version of the naming convention for the copies of this table.
        
        #execute 3 sql queries to create a full, snapshot and delta table in the local sct database
        execute_sql(server=pc_server, database=db, query=sql_query_make_full_snap_delta(original, naming, YYYYMMDD, db))
        logging.info(f'Full, Snapshot and Delta tables created in {db} from {original}, dated {YYYYMMDD}.')   


    logging.info('Full, delta and snapshot table creation complete. Ready for export...')