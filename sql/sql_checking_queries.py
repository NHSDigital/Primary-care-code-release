from src.utils.dates import class_Dates
from src.utils.databases import class_UKSNOMEDCT_dbs, class_LocalSNOMEDCT_dbs, class_ClusterManagement_dbs

#-------------------------------------------------------------------------------------------------------------------
#Module_dependeny.py
def sql_query_monolith_TRUD_clin_check(LocalSNOMEDCT:class_LocalSNOMEDCT_dbs, PCDreleaseDate):
    #selects the top 8 latest results 
    
    query = f"""SET NOCOUNT ON;
                SELECT [effectiveTime]
                    ,[active]
                    ,[moduleId]
                    ,[refsetId]
                    ,[referencedComponentId]
                    ,[sourceEffectiveTime]
                    ,[targetEffectiveTime]
                FROM {LocalSNOMEDCT.ModuleDependency}
                WHERE effectiveTime = '{PCDreleaseDate}'
                --and sourceEffectiveTime = '99999999'
                ORDER BY sourceEffectiveTime desc
                """
    return query

##-------------------------------------------------------------------------------------------------------------------

def sql_query_retired_clusters(ClusterManagement:class_ClusterManagement_dbs):
    query = f"""
        declare @now datetime;
        set @now = getdate()
        SELECT convert(VARCHAR(100), @now, 103);


        declare @lastfortnight datetime;
        set @lastfortnight = dateadd(day,-14,@now)
        SELECT convert(VARCHAR(100), @lastfortnight, 103);


        SELECT *
        FROM {ClusterManagement.Code_Decision_Log_Archive}
        WHERE [Final_Decision] = 'Retire'
        AND [Date_Actioned] BETWEEN @lastfortnight and @now """
    
    return query

#-------------------------------------------------------------------------------------------------------------------


##-------------------------------------------------------------------------------------------------------------------
#update_SCT.py
def sql_QA_query_PSU_counts(UKSNOMEDCT:class_UKSNOMEDCT_dbs):
    
    query = f"""
            SELECT COUNT(DISTINCT effectiveTime), COUNT(DISTINCT referencedComponentId)
            FROM {UKSNOMEDCT.SCT_REFSET}
            WHERE moduleId = '999000011000230102'
            """
    return query
#-------------------------------------------------------------------------------------------------------------------


##-------------------------------------------------------------------------------------------------------------------
#update_SCT.py
def sql_QA_query_TRUD_counts(LocalSNOMEDCT:class_LocalSNOMEDCT_dbs, configDates:class_Dates):
    
    query = f"""
            SELECT COUNT(DISTINCT effectiveTime), COUNT(DISTINCT referencedComponentId)
            FROM {LocalSNOMEDCT.db}.dbo.der2_Refset_SimpleSnapshot_1000230_{configDates.YYYYMMDD(configDates.PCDreleaseDate)}
            """
    return query
#-------------------------------------------------------------------------------------------------------------------


##-------------------------------------------------------------------------------------------------------------------
#Supporting_Products.py
def sql_test_query_B_RefData_creation(configDates:class_Dates):
    
    query = f"""
    select * from (
                   --check there are no rows with a null concept description
                   select 'Records with null code description' as [Table purpose], * 
                    from PSU_TRUD_{configDates.MmmYY_prev}_Code_Release.[dbo].GPData_Cluster_refset_1000230_{configDates.YYYYMMDD(configDates.PrevPCDrelDate)}
                    where ConceptId_Description is null 

                    UNION ALL
                    --check there are no more special characters
                    select 'Descriptions with special characters' as [Table purpose], * 
                    from  PSU_TRUD_{configDates.MmmYY_prev}_Code_Release.[dbo].GPData_Cluster_refset_1000230_{configDates.YYYYMMDD(configDates.PrevPCDrelDate)}
                    WHERE ConceptId_Description like '%Ã%' or Refset_Description like '%Ã%'

                    UNION ALL
                    --check there NULL cluster categories
                    select 'Cluster category is null' as [Table purpose], * 
                    from  PSU_TRUD_{configDates.MmmYY_prev}_Code_Release.[dbo].GPData_Cluster_refset_1000230_{configDates.YYYYMMDD(configDates.PrevPCDrelDate)}
                    WHERE Cluster_Category = 'NULL' or Cluster_Category is null
                    ) a
            """
    return query
#-------------------------------------------------------------------------------------------------------------------


##-------------------------------------------------------------------------------------------------------------------
#Supporting_Products.py
def sql_QA_query_GPData_count(db, PCDreleaseDate):

    query = f"""
            select Count (distinct Cluster_ID) as [Number of clusters], Count (Cluster_ID) as [Number of rows]
            from {db}.[dbo].GPData_Cluster_refset_1000230_{PCDreleaseDate}
            """
    return query
#-------------------------------------------------------------------------------------------------------------------


##-------------------------------------------------------------------------------------------------------------------
#TRUD_table_Cleardown.py
def sql_query_TRUD_table_check():
    #must use TRUD_PCD_Clinical_Refsets in connection because can't define in sql query or NoneType object not iterable error
    query = f"""
            SELECT table_name FROM INFORMATION_SCHEMA.TABLES
            WHERE table_name LIKE '%Snapshot_1000230%';
            """
    return query
#-------------------------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------

#-------------------------------------------------------------------------------------------------------------------
# Check Ruleset_Publised table is up to date before PowerBI script can be run
def sql_query_Ruleset_Pub_Update_check(ClusterManagement:class_ClusterManagement_dbs):
    query = f"""
    Select p.Service_ID, p.Ruleset_ID, p.Ruleset_Version, p.Ruleset_Publication_Effective, r.Ruleset_Date_Inactive, max(m.ruleset_version) as [Latest_version]
    ,CASE WHEN r.Ruleset_Date_Inactive is null THEN 'Active ruleset'
        WHEN r.Ruleset_Date_Inactive > getdate() THEN 'Service still ongoing - check next version published as well'
        ELSE 'ISSUE - service no longer active' End as [Publication_issues]
    From {ClusterManagement.Ruleset_Published} p
    Left join {ClusterManagement.Rulesets} r
        on p.Service_ID COLLATE Latin1_General_CI_AS = r.Service_ID
        and p.Ruleset_ID COLLATE Latin1_General_CI_AS = r.Ruleset_ID
        and p.Ruleset_Version = r.Ruleset_Version
    Left join {ClusterManagement.Rulesets} m
        on p.Service_ID COLLATE Latin1_General_CI_AS = m.Service_ID
        and p.Ruleset_ID COLLATE Latin1_General_CI_AS = m.Ruleset_ID
    Where Ruleset_Publication_Inactive is null
    Group by p.Service_ID, p.Ruleset_ID, p.Ruleset_Version, p.Ruleset_Publication_Effective, r.Ruleset_Date_Inactive
    Order by r.Ruleset_Date_Inactive,p.Ruleset_Publication_Effective,p.Service_ID, p.Ruleset_ID, p.Ruleset_Version
    """
    return query
#-------------------------------------------------------------------------------------------------------------------