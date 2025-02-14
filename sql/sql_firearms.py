from src.utils.dates import class_Dates
from src.utils.databases import class_UKSNOMEDCT_dbs, class_LocalSNOMEDCT_dbs

#90-------------------------------------------------------------------------------------------------------------------
#Supporting_Products.py
def sql_query_Firearms_ref_data(configDates:class_Dates, UKSNOMEDCT:class_UKSNOMEDCT_dbs, LocalSNOMEDCT:class_LocalSNOMEDCT_dbs):
    
    query = f"""
            SET NOCOUNT ON; SET ANSI_WARNINGS OFF

            DROP TABLE IF EXISTS {UKSNOMEDCT.db}.dbo.Firearms_{configDates.YYYYMMDD(configDates.PCDreleaseDate)}

            --Setup basic info
            Select r.referencedComponentId,r.active as [currentMembership]
            , c.active as [conceptStatus], d.term as [fullySpecifiedName]
            , n.term as [refsetName], r.refsetId
            into #TT_basicinfo
            from {UKSNOMEDCT.SCT_REFSET} r
            left join {UKSNOMEDCT.SCT_CONCEPT} c
                on r.referencedComponentId = c.id
            left join {UKSNOMEDCT.SCT_DESCRIPTION} d
                on r.referencedComponentId = d.conceptId and d.active = 1 and d.DescriptionType = 'F'
            left join {UKSNOMEDCT.SCT_DESCRIPTION} n
                on r.refsetId = n.conceptId and n.active = 1 and n.DescriptionType = 'F'
            where r.refsetId = '999031731000230105'

            --Set up first membership info
            Select refsetId, referencedComponentId, min(effectiveTime) as [firstMembershipTime], active 
            into #TT_first_time
            from {LocalSNOMEDCT.Refsets} t
            where t.refsetId = '999031731000230105' and t.active = 1
            group by refsetId, referencedComponentId, active

            --join together
            Select b.referencedComponentId
            ,f.firstMembershipTime
            ,CASE WHEN b.currentMembership = 1 THEN 'member' 
                WHEN b.currentMembership = 0 THEN 'exmember' 
                ELSE 'unknown' END as [currentMembership]
            ,CASE WHEN b.conceptStatus = 1 THEN 'active' 
                WHEN b.conceptStatus = 0 THEN 'inactive' 
                ELSE 'unknown' END as [conceptStatus]
            ,b.fullySpecifiedName
            ,b.refsetName
            ,b.refsetId
            into {UKSNOMEDCT.db}.dbo.Firearms_{configDates.YYYYMMDD(configDates.PCDreleaseDate)}
            from #TT_basicinfo b
            left join #TT_first_time f
            on b.referencedComponentId = f.referencedComponentId and b.refsetId = f.refsetId
            order by currentMembership desc, conceptStatus, fullySpecifiedName

            --This table should be added to the Firearms Excel template and published in the supporting products folder of the code release. 

            SELECT * FROM {UKSNOMEDCT.db}.dbo.Firearms_{configDates.YYYYMMDD(configDates.PCDreleaseDate)}
            order by currentMembership desc, conceptStatus, fullySpecifiedName
            """
    return query
#-------------------------------------------------------------------------------------------------------------------
