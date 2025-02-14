from src.utils.databases import class_UKSNOMEDCT_dbs, class_LocalSNOMEDCT_dbs, class_ClusterManagement_dbs

#GDPPR_SPL_Content.py
#14-------------------------------------------------------------------------------------------------------------------
def sql_query_GDPPR_C19SPL_changes(UKSNOMEDCT:class_UKSNOMEDCT_dbs, LocalSNOMEDCT:class_LocalSNOMEDCT_dbs, ClusterManagement:class_ClusterManagement_dbs):
    
    query = f"""
        SET NOCOUNT ON; 

        --Summarise GDPPR and C19SPL changes for the analysts
        DECLARE @PublicationDate as varchar(8)
        SET @PublicationDate = (Select max(effectiveTime) as [date] from {LocalSNOMEDCT.ModuleDependency})

        --1) Get maximum cluster versions for all SPL and GDPPR clusters and their refset IDs (apart from GDPPR_COD and GDPPR2YR_COD)
        DROP TABLE if exists #TT_COVID_Clusters
        Select distinct a.Ruleset_ID, a.Ruleset_Version, a.Cluster_ID, c.Refset_ID
        into #TT_COVID_Clusters
        from {ClusterManagement.Cluster_Ruleset} a
        inner join {ClusterManagement.Clusters} c	on a.Cluster_ID = c.Cluster_ID and c.Cluster_Date_Inactive is null
        where	
        (  (Ruleset_ID = 'C19SPL' and Ruleset_Version >= 5.0)	--***IMPORTANT! - This needs updating when BRs change. 
        or (Ruleset_ID = 'GDPPR' and Ruleset_Version >= 3.1)
        ) 
        AND a.Cluster_ID NOT IN ('GDPPR_COD','GDPPR2YR_COD')

        --3) Get same info but FOR GDPPR_COD and GDPPR2YR_COD
        DROP TABLE if exists #TT_GDPPR_Clusters
        Select distinct b.Cluster_ID, b.Refset_ID
        into #TT_GDPPR_Clusters 
        from {ClusterManagement.Clusters} b
        where b.Cluster_ID IN ('GDPPR_COD','GDPPR2YR_COD')
        and b.Cluster_Date_Inactive is null

        --select * from #TT_GDPPR_Clusters g

        --4) Check which changes happened to GDPPR clusters - That is the big mega clusters themselves.
        Drop table if exists #TT_GDPPR_Link
        Select distinct
        t.effectiveTime			as [Date_change_published],
        t.referencedComponentId as [SNOMED_code], 
        CASE WHEN t.active = 1 then 'added to' Else 'removed from' END as [added/removed],
        g.Cluster_ID			as [Cluster]
        into #TT_GDPPR_Link
        from {LocalSNOMEDCT.Refsets} t
        inner join #TT_GDPPR_Clusters g on t.refsetId = g.Refset_ID
        where effectiveTime = @PublicationDate

        --5) Check which changes happened to GDPPR underlying clusters - i.e. the underlying clusters that the big clusters are based on.
        Drop table if exists #TT_GDPPR_UL
        Select distinct
        t.effectiveTime			as [Date_change_published],
        t.referencedComponentId as [SNOMED_code], 
        CASE WHEN t.active = 1 then 'added to' Else 'removed from' END as [added/removed],
        g.Cluster_ID			as [Cluster]
        into #TT_GDPPR_UL
        from {LocalSNOMEDCT.Refsets} t
        inner join #TT_COVID_Clusters g on t.refsetId = g.Refset_ID 
        where effectiveTime = @PublicationDate and Ruleset_ID = 'GDPPR'

        --6) Check which changes happened to C19SPL clusters
        Drop table if exists #TT_SPL_Link
        Select distinct
        t.effectiveTime			as [Date_change_published],
        t.referencedComponentId as [SNOMED_code], 
        CASE WHEN t.active = 1 then 'added to' Else 'removed from' END as [added/removed],
        g.Cluster_ID			as [Cluster]
        into #TT_SPL_Link
        from {LocalSNOMEDCT.Refsets} t
        inner join #TT_COVID_Clusters g on t.refsetId = g.Refset_ID 
        where effectiveTime = @PublicationDate and Ruleset_ID = 'C19SPL'

        --Summarise the info from both rulesets
        Select * into #TT_All_Records from 

        (--GDPPR clusters
        Select 'GDPPR' as [Ruleset], a.SNOMED_code, a.[added/removed], a.Cluster, b.Cluster as [due to underlying changes in], @PublicationDate as [Date_changes_published]
        from #TT_GDPPR_Link a
        left join #TT_GDPPR_UL b 
        on a.SNOMED_code = b.SNOMED_code and a.[added/removed] = b.[added/removed]

        UNION ALL 

        --C19SPL changes
        Select 'C19SPL' as [Ruleset],a.SNOMED_code, a.[added/removed], a.Cluster,  'n/a' as [due to underlying changes in], @PublicationDate as [Date_changes_published]
        from #TT_SPL_Link a

        ) z

        Select a.Ruleset, a.SNOMED_code, d.term as [Code_description], a.[added/removed], a.[due to underlying changes in], a.Date_changes_published
        from #TT_All_Records a
        left join {UKSNOMEDCT.SCT_DESCRIPTION} d
        on a.SNOMED_code = d.conceptId and d.active = 1 and d.DescriptionType = 'F'

            """
    return query
#-------------------------------------------------------------------------------------------------------------------
