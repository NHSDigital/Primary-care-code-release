from src.utils.dates import class_Dates
from src.utils.databases import class_UKSNOMEDCT_dbs, class_LocalSNOMEDCT_dbs

##-------------------------------------------------------------------------------------------------------------------
#Module_dependency.py
def sql_query_pd_actual_md(configDates:class_Dates, UKSNOMEDCT:class_UKSNOMEDCT_dbs, LocalSNOMEDCT:class_LocalSNOMEDCT_dbs):
    
    query = f"""
    Select distinct 
    CASE WHEN c.moduleId IN ('900000000000207008','900000000000012004') THEN 'International'
    WHEN c.moduleId IN ('999000041000000102','83821000000107') THEN 'UK'
    WHEN c.moduleId IN ('999000011000001104','999000021000001108') THEN 'Drug'
    WHEN c.moduleId IN ('999000011000000103','999000021000000109') THEN 'UK other'
    WHEN c.moduleId IN ('1326031000000103','') THEN 'Pathology'
    ELSE 'Other'
    END as [ReleaseType]
    ,c.moduleId, max(m.sourceEffectiveTime) as [sourceEffectiveTime]
    from {LocalSNOMEDCT.Refsets} f
    left join {UKSNOMEDCT.SCT_CONCEPT} c
    on f.referencedComponentId = c.id
    left join {UKSNOMEDCT.live}.dbo.der2_ssRefset_ModuleDependencyMONOSnapshot_GB_{configDates.UKreleaseDate} m
    on c.moduleId = m.moduleId and m.active = '1'
    group by c.moduleId
    order by ReleaseType

            """
    return query
#-------------------------------------------------------------------------------------------------------------------

##-------------------------------------------------------------------------------------------------------------------
#Module_dependeny.py
def sql_query_monolith_replace_tar(v_moduleID, v_refsetID, x_value, x_date, LocalSNOMEDCT:class_LocalSNOMEDCT_dbs):
    
    
    query = f"""SET NOCOUNT ON;
    
    IF NOT EXISTS (
                    Select * from {LocalSNOMEDCT.ModuleDependency}
                    where 
                    effectiveTime = '99999999'
                    and active = '1'
                    and moduleId = '{v_moduleID}'
                    and refsetId = '{v_refsetID}'
                    and referencedComponentId = '{x_value}'
                    and sourceEffectiveTime = '99999999'
                    and targetEffectiveTime = '{x_date}')
    BEGIN
            INSERT INTO {LocalSNOMEDCT.ModuleDependency}
            Select Unique_ID_Generated as [id]
            ,'99999999' as [effectiveTime] 
            ,'1' as [active]
            ,moduleId as [moduleId]
            ,refsetId as [refsetId]
            ,conceptId as [referencedComponentId] 
            ,'99999999' as [sourceEffectiveTime]
            ,'{x_date}' as [targetEffectiveTime]
            from {LocalSNOMEDCT.TRUD_ID_Gen} b
            where moduleId = '{v_moduleID}' and refsetId = '{v_refsetID}' and conceptId = '{x_value}'
    END
            """
    return query
#-------------------------------------------------------------------------------------------------------------------

##-------------------------------------------------------------------------------------------------------------------
#Module_dependency.py
def sql_query_monolith_exists(configDates:class_Dates, UKSNOMEDCT:class_UKSNOMEDCT_dbs):
    
    query = f"""
            IF OBJECT_ID (N'{UKSNOMEDCT.live}.dbo.der2_ssRefset_ModuleDependencyMONOSnapshot_GB_{configDates.UKreleaseDate}', N'U') IS NOT NULL
                Select 1 --as [TableExists]
            ELSE
                Select 0 --as [TableExists]
            """
    return query
#-