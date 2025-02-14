
###<<<Defined Code release SQL queries>>>
from src.utils.dates import class_Dates
from src.utils.databases import class_LocalSNOMEDCT_dbs


##-------------------------------------------------------------------------------------------------------------------
#export_tables.py
def sql_query_select_distinct(database, table_name):
    # db: TRUD_PCD_Clinical_Refsets
    query = f"""
            SELECT DISTINCT *
            FROM [{database}].[dbo].[{table_name}]
            """
    return query

##-------------------------------------------------------------------------------------------------------------------
#Supporting_Products.py
def sql_query_select_all(database, table_name): #doesn't use configDates parameter since in test mode prevMmmYY and prevPCDrelDate are used instead
    # from PSU_TRUD_{MmmYY}_Code_Release.[dbo].GPData_Cluster_refset_1000230_{PCDreleaseDate}
    query = f"""   
            select *
            from {database}.dbo.{table_name}
        """
    return query
#-------------------------------------------------------------------------------------------------------------------
#expanded_clusters_changes.py
def sql_query_select_all_short(db_and_table_name): 
    query = f"""   
            select *
            from {db_and_table_name}
        """
    return query
##-------------------------------------------------------------------------------------------------------------------
#SupportingInfo.py
def sql_query_add_remov_refset(LocalSNOMEDCT:class_LocalSNOMEDCT_dbs):
        
    query = f"""Select active as [active status], count(id) as [refsets]
                            from {LocalSNOMEDCT.Concept}
                            where effectiveTime = (
                                                    Select max(sourceEffectiveTime) as [PCD_Date]
                                                    from {LocalSNOMEDCT.ModuleDependency}
                                                    where moduleId = '999000011000230102'
                                                    )
                            group by active
            """
    return query
#-------------------------------------------------------------------------------------------------------------------

##-------------------------------------------------------------------------------------------------------------------
#SupportingInfo.py
def sql_query_TRUD_total_refsets(table): # db and tbl name used in log, hence read in
    
    
    query = f"""
            Select active, count(id) as [active_refsets]
            from {table}
            where active = 1
            group by active
            """
    return query
#-------------------------------------------------------------------------------------------------------------------
