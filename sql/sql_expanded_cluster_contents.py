from src.utils.dates import class_Dates
from src.utils.databases import class_UKSNOMEDCT_dbs, class_LocalSNOMEDCT_dbs, class_ClusterManagement_dbs

#-------------------------------------------------------------------------------------------------------------------

def sql_query_latest_ECL_cluster_additions(cluster, UKSNOMEDCT:class_UKSNOMEDCT_dbs, LocalSNOMEDCT:class_LocalSNOMEDCT_dbs, ClusterManagement:class_ClusterManagement_dbs):
    """
    Function Actions:
    - -- ====================================================================================================
    -- Refset_last_update
    -- Description:	Returns the date the cluster was last updated along with details of the addition changes made
    -- ====================================================================================================
    """

    query = f"""

    Select distinct b.Cluster_ID, a.[refsetId] as [Refset_ID], a.[referencedComponentId] as [SNOMED concept ID], d.term as [Code description], a.[active] as [Active status], a.effectiveTime
    From {LocalSNOMEDCT.Refsets} as a
    Inner Join (
                select distinct Cluster_ID, Refset_ID, max([Cluster_Date_Effective]) as MaxDate
                from {ClusterManagement.Clusters}
                where Cluster_ID = {cluster} --Enter Cluster ID
                group by Cluster_ID, Refset_ID
                ) as b
    On a.refsetId = b.Refset_ID
    Inner Join (
                select distinct refsetId, max(effectiveTime) as [MaxDate]
                from {LocalSNOMEDCT.Refsets}
                group by refsetId
                ) as c
    On a.refsetId = c.refsetId AND a.effectiveTime = c.MaxDate
    Left Join {UKSNOMEDCT.SCT_DESCRIPTION_prefix} as d -- Replace MmmYY with latest code release month and year
    on a.referencedComponentId = d.conceptId and d.active = 1 and d.DescriptionType = 'F'
    Order by a.referencedComponentId
    """
    return(query)

#-------------------------------------------------------------------------------------------------------------------


def sql_query_return_curr_prior_post_release_date(LocalSNOMEDCT: class_LocalSNOMEDCT_dbs):

    query = f"""
	SELECT DISTINCT TOP 2 effectiveTime as [Expanded cluster list are based on the following PCD TRUD releases:]
	From (SELECT DISTINCT TOP 2 effectiveTime from {LocalSNOMEDCT.ModuleDependency}) x
	ORDER BY effectiveTime DESC

    """

    return(query)

#-------------------------------------------------------------------------------------------------------------------

def sql_query_all_code_rem_add_post_release(PCDreleaseDate, LocalSNOMEDCT: class_LocalSNOMEDCT_dbs, UKSNOMEDCT: class_UKSNOMEDCT_dbs, ClusterManagement:class_ClusterManagement_dbs):
    """
    Function Actions:
    - -- ====================================================================================================
    -- Description: Returns a dataframe of all the latest "addition" and "removal" code changes in the last code release
    -- ====================================================================================================
    """
 
 
    query = f"""
        SET NOCOUNT ON;
 
        -- summary
        DROP TABLE IF EXISTS #TT_all_release_changes
        DROP TABLE IF EXISTS #TT_content
 
        select distinct refsetId, referencedComponentId, max(active) as active, max(effectiveTime) as maxeffectiveTime
        , case when max(effectiveTime) = '{PCDreleaseDate}' and max(active) = 1 then 'addition'
        when max(effectiveTime) = '{PCDreleaseDate}' and max(active) = 0 then 'removal'
        else 'no change' end as [change]
        into #TT_all_release_changes
        from {LocalSNOMEDCT.db}.[dbo].[der2_Refset_SimpleSnapshot_1000230_{PCDreleaseDate}]
        group by refsetId, referencedComponentId
        order by change
 
       
        Select distinct
            p.Service_ID + '_' + p.Ruleset_ID as [Service_Ruleset],
            p.Service_ID,
            p.Ruleset_ID,
            p.Ruleset_Version,
            g.refsetId as [Refset_ID],
            a.Cluster_ID as [Cluster_ID],
            a.Cluster_Description as [Cluster_description],
            g.referencedComponentId as [SNOMED_concept_ID],
            d.term as [Code_description],
            g.maxeffectiveTime,
            g.change,
            c.active as [code_active]
        INTO #TT_content
        from #TT_all_release_changes as g
        left join {ClusterManagement.Clusters} as a
        on a.Refset_ID = g.refsetId
        left join {UKSNOMEDCT.SCT_DESCRIPTION} as d
        on g.referencedComponentId = d.conceptId and d.DescriptionType = 'F'
        left join {UKSNOMEDCT.SCT_CONCEPT} as c
        on g.referencedComponentId = c.id
        left join (select *
                    from {ClusterManagement.Cluster_Ruleset}
                    where Cluster_Ruleset_Date_Inactive is null
                    or Cluster_Ruleset_Date_Inactive > GETDATE() ) p --dates after today to include service currently active
        on a.Cluster_ID = p.Cluster_ID
        WHERE g.change in ('addition', 'removal')
        ORDER BY 2,4
 
        SELECT *
        FROM #TT_content
    """
   
    return(query)

#-------------------------------------------------------------------------------------------------------------------


def sql_query_all_cluster_changes(LocalSNOMEDCT, UKSNOMEDCT, ClusterManagement, configDates):
    query = f"""
        

        --=================================================================================================================================================
        -- Creation_of_expanded_cluster_list_RULESET_LEVEL for both current and prior code releases
        -- Description:	Creates an expanded cluster list at ruleset-level, that contains the changes to the current and prior ECL (adds, removes, no-changes)

        --=================================================================================================================================================
        -- Step 1: Define vairables
        --=================================================================================================================================================
        
        SET ANSI_WARNINGS OFF
        SET NOCOUNT ON;
              
              
    

        --=================================================================================================================================================
        -- Step 2: drop temporary table where it exists if this code has already been run
        --=================================================================================================================================================
        DROP TABLE IF EXISTS #TT_Clusters
        DROP TABLE IF EXISTS #TT_RefsetContent_old
        DROP TABLE IF EXISTS #TT_RefsetContent_new
        DROP TABLE IF EXISTS #TT_ECL_comparison
        DROP TABLE IF EXISTS #TT_latest_changes

        --=================================================================================================================================================
        -- Step 3: Create a temporary table containing all clusters and refsets used in the selected service. 
        --		   This is drawn from information held in the Cluster Management database.
        --=================================================================================================================================================

        Select distinct cr.Cluster_ID, c.Cluster_Description, c.Refset_ID 
        into #TT_Clusters
        from {ClusterManagement.Cluster_Ruleset} cr
        inner join {ClusterManagement.Clusters} c
        on cr.Cluster_ID = c.Cluster_ID
        where c.Cluster_Date_Inactive is null    

        --=================================================================================================================================================
        -- Step 4: 	Return previous and new refset content for the rulesets by joining the relevant primary care and monolith simple refset snapshot tables together. 
        --=================================================================================================================================================
        --new refset content
        Select * into #TT_RefsetContent_new
        from (
            Select distinct * 
            from {LocalSNOMEDCT.db}.dbo.der2_Refset_SimpleSnapshot_1000230_{configDates.PCDreleaseDate} --latest pcd release date (i.e. the one we are currently publishing)
            union all
            Select * from {UKSNOMEDCT.db}.dbo.der2_Refset_SimpleMONOSnapshot_GB_{configDates.UKreleaseDate} -- monolith date the above is based on
            where moduleId <> '999000011000230102' --pcd release
            ) r
        inner join #TT_Clusters c
        on r.refsetId = c.Refset_ID
        where r.active = 1
                
        --previous refset content
        Select * into #TT_RefsetContent_old
        from (
            Select distinct * 
            from {LocalSNOMEDCT.db}.dbo.der2_Refset_SimpleSnapshot_1000230_{configDates.PrevPCDrelDate} --previous pcd release date
            union all
            Select id COLLATE DATABASE_DEFAULT,
                   effectiveTime COLLATE DATABASE_DEFAULT,
                   active COLLATE DATABASE_DEFAULT,
                   moduleId COLLATE DATABASE_DEFAULT,
                   refsetId COLLATE DATABASE_DEFAULT,
                   referencedComponentId COLLATE DATABASE_DEFAULT 
            from {UKSNOMEDCT.db}.dbo.der2_Refset_SimpleMONOSnapshot_GB_{configDates.PrevUKrelDate} --monolith that the previous pcd release was based on
            where moduleId <> '999000011000230102' --pcd release
            ) r
        inner join #TT_Clusters c
        on r.refsetId = c.Refset_ID
        where r.active = 1

        --=================================================================================================================================================
        -- Step 5: Produces a summary column of which codes have been added or removed since the last code release
        --=================================================================================================================================================
        Select distinct
        case when o.Cluster_ID COLLATE Latin1_General_CI_AS is null then n.Cluster_ID COLLATE Latin1_General_CI_AS else o.Cluster_ID COLLATE Latin1_General_CI_AS end as [Cluster_ID]
        ,case when o.Cluster_Description COLLATE Latin1_General_CI_AS is null then n.Cluster_Description COLLATE Latin1_General_CI_AS else o.Cluster_Description COLLATE Latin1_General_CI_AS  end as [Cluster_Description]
        ,case when o.Refset_ID COLLATE Latin1_General_CI_AS is null then n.Refset_ID COLLATE Latin1_General_CI_AS else o.Refset_ID COLLATE Latin1_General_CI_AS  end as [Refset_ID]
        ,case when o.referencedComponentId COLLATE Latin1_General_CI_AS is null then n.referencedComponentId COLLATE Latin1_General_CI_AS else o.referencedComponentId COLLATE Latin1_General_CI_AS  end as [SNOMED_concept_ID]
        ,case when o.referencedComponentId COLLATE Latin1_General_CI_AS is null and n.referencedComponentId COLLATE Latin1_General_CI_AS is not null then 'added'--'New code added: '+n.referencedComponentId
            when o.referencedComponentId COLLATE Latin1_General_CI_AS is not null and n.referencedComponentId COLLATE Latin1_General_CI_AS is not null then 'no change'--'In both ECLs'
            when o.referencedComponentId COLLATE Latin1_General_CI_AS is not null and n.referencedComponentId COLLATE Latin1_General_CI_AS is null then 'removed'--'Code has been removed: '+o.referencedComponentId end
            end as [Outcome]
        into #TT_latest_changes
        from #TT_RefsetContent_old as o
        full outer join #TT_RefsetContent_new as n
        on o.referencedComponentId  COLLATE Latin1_General_CI_AS  = n.referencedComponentId  COLLATE Latin1_General_CI_AS 
                and o.Cluster_ID = n.Cluster_ID

		
		--=================================================================================================================================================
        -- Step 7:	Neatened version with extra info (i.e. final expanded cluster list with changes)
        --=================================================================================================================================================
            SELECT [Cluster_ID]
            ,l.[Cluster_description]
            ,[SNOMED_concept_ID]
            ,d.term as [Code_description]
            ,case when l.Refset_ID like '%100000110%' then 'Drug Refset'
                    when l.Refset_ID like '%100023010%' then 'PCD Refset'
                    else 'Refset' end as [Type_of_Refset]
            ,'^'+l.Refset_ID as [Cluster_Code_String]
            ,c.active as [Active_status]
            ,[Outcome]
            INTO #TT_Final
            FROM #TT_latest_changes l
            left join {UKSNOMEDCT.SCT_DESCRIPTION} d
            on l.SNOMED_concept_ID COLLATE DATABASE_DEFAULT  = d.conceptId  and d.DescriptionType = 'F'  and d.active = 1 --COLLATE DATABASE_DEFAULT
            left join {UKSNOMEDCT.SCT_CONCEPT} c
            on l.SNOMED_concept_ID COLLATE DATABASE_DEFAULT  = c.id  
			ORDER BY outcome, cluster_ID ASC
 

            Select * from #TT_Final

            """
    
    return query
#-------------------------------------------------------------------------------------------------------------------
def sql_query_changed_with_outcomes_ruleset_ECC_tbl(Ruleset_ID, Ruleset_version, Service_ID, LocalSNOMEDCT: class_LocalSNOMEDCT_dbs, UKSNOMEDCT: class_UKSNOMEDCT_dbs,  ClusterManagement:class_ClusterManagement_dbs, configDates: class_Dates):


    query = f"""
        --=================================================================================================================================================
        -- Creation_of_expanded_cluster_list_RULESET_LEVEL for both current and prior code releases
        -- Description:	Creates an expanded cluster list at ruleset-level, that contains the changes to the current and prior ECL (adds, removes, no-changes)

        --=================================================================================================================================================
        -- Step 1: Define vairables
        --=================================================================================================================================================
        SET NOCOUNT ON;
                
        DECLARE @Ruleset_ID varchar(50),
                @Ruleset_Version decimal(9,1),
                @Service_ID varchar(50)
                        
        SET @Ruleset_ID = '{Ruleset_ID}'		
        SET @Ruleset_Version = {Ruleset_version}					
        SET @Service_ID = '{Service_ID}'

        --=================================================================================================================================================
        -- Step 2: drop temporary table where it exists if this code has already been run
        --=================================================================================================================================================
        DROP TABLE IF EXISTS #TT_Clusters
        DROP TABLE IF EXISTS #TT_RefsetContent_old
        DROP TABLE IF EXISTS #TT_RefsetContent_new
        DROP TABLE IF EXISTS #TT_ECL_comparison
        DROP TABLE IF EXISTS #TT_latest_changes

        --=================================================================================================================================================
        -- Step 3: Create a temporary table containing all clusters and refsets used in the selected ruleset. 
        --		   This is drawn from information held in the Cluster Management database.
        --=================================================================================================================================================
        Select cr.Cluster_ID, c.Cluster_Description, c.Refset_ID 
        into #TT_Clusters
        from {ClusterManagement.Cluster_Ruleset} cr
        inner join {ClusterManagement.Clusters} c
        on cr.Cluster_ID = c.Cluster_ID
        where cr.Ruleset_ID = @Ruleset_ID
        and Ruleset_Version = @Ruleset_Version   --This is a ruleset level query for a single ruleset so no need for flooring here
        and c.Cluster_Date_Inactive is null
    
        --=================================================================================================================================================
        -- Step 4: 	Return previous and new refset content for the rulesets by joining the relevant primary care and monolith simple refset snapshot tables together. 
        --=================================================================================================================================================
        --new refset content
        Select * into #TT_RefsetContent_new
        from (
            Select distinct * 
            from {LocalSNOMEDCT.db}.dbo.der2_Refset_SimpleSnapshot_1000230_{configDates.PCDreleaseDate} --latest pcd release date (i.e. the one we are currently publishing)
            union all
            Select * from {UKSNOMEDCT.db}.dbo.der2_Refset_SimpleMONOSnapshot_GB_{configDates.UKreleaseDate} --monolith date the above is based on
            where moduleId <> '999000011000230102' --pcd release
            ) r
        inner join #TT_Clusters c
        on r.refsetId = c.Refset_ID
        where r.active = 1
                
        --previous refset content
        Select * into #TT_RefsetContent_old
        from (
            Select distinct * 
            from {LocalSNOMEDCT.db}.dbo.der2_Refset_SimpleSnapshot_1000230_{configDates.PrevPCDrelDate} --previous pcd release date
            union all
            Select * from {UKSNOMEDCT.db}.dbo.der2_Refset_SimpleMONOSnapshot_GB_{configDates.PrevUKrelDate} --monolith that the previous pcd release was based on
            where moduleId <> '999000011000230102' --pcd release
            ) r
        inner join #TT_Clusters c
        on r.refsetId = c.Refset_ID
        where r.active = 1

        --=================================================================================================================================================
        -- Step 5: Produces a summary column of which codes have been added or removed since the last code release
        --=================================================================================================================================================
        Select distinct
        case when o.Cluster_ID is null then n.Cluster_ID else o.Cluster_ID end as [Cluster_ID]
        ,case when o.Cluster_Description is null then n.Cluster_Description else o.Cluster_Description end as [Cluster_Description]
        ,case when o.Refset_ID is null then n.Refset_ID else o.Refset_ID end as [Refset_ID]
        ,case when o.referencedComponentId is null then n.referencedComponentId else o.referencedComponentId end as [SNOMED_concept_ID]
        ,case when o.referencedComponentId is null and n.referencedComponentId is not null then 'added'--'New code added: '+n.referencedComponentId
            when o.referencedComponentId is not null and n.referencedComponentId is not null then 'no change'--'In both ECLs'
            when o.referencedComponentId is not null and n.referencedComponentId is null then 'removed'--'Code has been removed: '+o.referencedComponentId end 
            end as [Outcome]
        into #TT_latest_changes
        from #TT_RefsetContent_old as o
        full outer join #TT_RefsetContent_new as n
        on o.referencedComponentId = n.referencedComponentId and o.Cluster_ID = n.Cluster_ID


        --=================================================================================================================================================
        -- Step 6:	Neatened version with extra info (i.e. final expanded cluster list with changes)
        --=================================================================================================================================================
            SELECT  @Service_ID as [Service_ID]
                ,@Ruleset_ID as [Ruleset]
                ,@Ruleset_Version as [Ruleset_Version]
                ,'SNOMED CT' as [Coding_ID]
                ,[Cluster_ID]
                ,[Cluster_description]
                ,[SNOMED_concept_ID]
                ,d.term as [Code_description]
                ,case when l.Refset_ID like '%100000110%' then 'Drug Refset'
                        when l.Refset_ID like '%100023010%' then 'PCD Refset'
                        else 'Refset' end as [Type_of_Refset]
                ,'^'+l.Refset_ID as [Cluster_Code_String]
                ,c.active as [Active_status]
                ,[Outcome]
        FROM #TT_latest_changes l
        left join {UKSNOMEDCT.SCT_DESCRIPTION} d
        on l.SNOMED_concept_ID = d.conceptId and d.DescriptionType = 'F' and d.active = 1
        left join {UKSNOMEDCT.SCT_CONCEPT} c
        on l.SNOMED_concept_ID = c.id 
        ORDER BY Cluster_ID ASC


    """
    
    return query



def sql_query_changed_with_outcomes_service_ECC_tbl(Ruleset_version, Service_ID, UKSNOMEDCT: class_UKSNOMEDCT_dbs, ClusterManagement:class_ClusterManagement_dbs, LocalSNOMEDCT: class_LocalSNOMEDCT_dbs, configDates: class_Dates):

    
    query = f"""
        --=================================================================================================================================================
        -- Step 1: Define vairables
        --=================================================================================================================================================
        SET NOCOUNT ON;
        
        DECLARE @Ruleset_Version decimal(9,1),
                @Service_ID varchar(50),
				@int_version as int
                
        SET @Ruleset_Version = {Ruleset_version} 	
        SET @Service_ID = '{Service_ID}' 
		SET @int_version = FLOOR(@Ruleset_Version) --	 (floor rounds down to nearest integer) 
                        
        --=================================================================================================================================================
        -- Step 2: drop temporary table where it exists if this code has already been run
        --=================================================================================================================================================
        DROP TABLE IF EXISTS #TT_Clusters
        DROP TABLE IF EXISTS #TT_RefsetContent_old
        DROP TABLE IF EXISTS #TT_RefsetContent_new
        DROP TABLE IF EXISTS #TT_ECL_comparison
        DROP TABLE IF EXISTS #TT_latest_changes

        --=================================================================================================================================================
        -- Step 3: Create a temporary table containing all clusters and refsets used in the selected service. 
        --		   This is drawn from information held in the Cluster Management database.
        --=================================================================================================================================================
        Select cr.Cluster_ID, c.Cluster_Description, c.Refset_ID 
        into #TT_Clusters
        from {ClusterManagement.Cluster_Ruleset} cr
        inner join {ClusterManagement.Clusters} c
        on cr.Cluster_ID = c.Cluster_ID
        where cr.Service_ID = @Service_ID
        and Ruleset_Version >= @int_version
        and Ruleset_Version < (@int_version + 1)    --This is service level which might have multiple ruleset versions so floor is needed
        and c.Cluster_Date_Inactive is null

        --=================================================================================================================================================
        -- Step 4: 	Return previous and new refset content for the rulesets by joining the relevant primary care and monolith simple refset snapshot tables together. 
        --=================================================================================================================================================
        --new refset content
        Select * into #TT_RefsetContent_new
        from (
            Select distinct * 
            from {LocalSNOMEDCT.db}.dbo.der2_Refset_SimpleSnapshot_1000230_{configDates.PCDreleaseDate} --latest pcd release date (i.e. the one we are currently publishing)
            union all
            Select * from {UKSNOMEDCT.db}.dbo.der2_Refset_SimpleMONOSnapshot_GB_{configDates.UKreleaseDate} -- monolith date the above is based on
            where moduleId <> '999000011000230102' --pcd release
            ) r
        inner join #TT_Clusters c
        on r.refsetId = c.Refset_ID
        where r.active = 1
                
        --previous refset content
        Select * into #TT_RefsetContent_old
        from (
            Select distinct * 
            from {LocalSNOMEDCT.db}.dbo.der2_Refset_SimpleSnapshot_1000230_{configDates.PrevPCDrelDate} --previous pcd release date
            union all
            Select * from {UKSNOMEDCT.db}.dbo.der2_Refset_SimpleMONOSnapshot_GB_{configDates.PrevUKrelDate} --monolith that the previous pcd release was based on
            where moduleId <> '999000011000230102' --pcd release
            ) r
        inner join #TT_Clusters c
        on r.refsetId = c.Refset_ID
        where r.active = 1

        --=================================================================================================================================================
        -- Step 5: Produces a summary column of which codes have been added or removed since the last code release
        --=================================================================================================================================================
        Select distinct
        case when o.Cluster_ID is null then n.Cluster_ID else o.Cluster_ID end as [Cluster_ID]
        ,case when o.Cluster_Description is null then n.Cluster_Description else o.Cluster_Description end as [Cluster_Description]
        ,case when o.Refset_ID is null then n.Refset_ID else o.Refset_ID end as [Refset_ID]
        ,case when o.referencedComponentId is null then n.referencedComponentId else o.referencedComponentId end as [SNOMED_concept_ID]
        ,case when o.referencedComponentId is null and n.referencedComponentId is not null then 'added'--'New code added: '+n.referencedComponentId
            when o.referencedComponentId is not null and n.referencedComponentId is not null then 'no change'--'In both ECLs'
            when o.referencedComponentId is not null and n.referencedComponentId is null then 'removed'--'Code has been removed: '+o.referencedComponentId end 
            end as [Outcome]
        into #TT_latest_changes
        from #TT_RefsetContent_old as o
        full outer join #TT_RefsetContent_new as n
        on o.referencedComponentId = n.referencedComponentId and o.Cluster_ID = n.Cluster_ID


        --=================================================================================================================================================
        -- Step 6:	Neatened version with extra info (i.e. final expanded cluster list with changes)
        --=================================================================================================================================================
            SELECT  @Service_ID as [Service_ID]
                ,@int_version as [Ruleset_Version]
                ,'SNOMED CT' as [Coding_ID]
                ,[Cluster_ID]
                ,[Cluster_description]
                ,[SNOMED_concept_ID]
                ,d.term as [Code_description]
                ,case when l.Refset_ID like '%100000110%' then 'Drug Refset'
                        when l.Refset_ID like '%100023010%' then 'PCD Refset'
                        else 'Refset' end as [Type_of_Refset]
                ,'^'+l.Refset_ID as [Cluster_Code_String]
                ,c.active as [Active_status]
                ,[Outcome]
        FROM #TT_latest_changes l
        left join {UKSNOMEDCT.SCT_DESCRIPTION} d
        on l.SNOMED_concept_ID = d.conceptId and d.DescriptionType = 'F' and d.active = 1
        left join {UKSNOMEDCT.SCT_CONCEPT} c
        on l.SNOMED_concept_ID = c.id 
        ORDER BY Cluster_ID ASC
        """
    return(query)

#-------------------------------------------------------------------------------------------------------------------

def all_live_upcoming_rulesets(ClusterManagement:class_ClusterManagement_dbs):
 
    query = f"""select
        CASE WHEN r.Ruleset_Date_Effective <= getdate() and (r.Ruleset_Date_Inactive >= getdate() or r.Ruleset_Date_Inactive is null) THEN 'Live'
            WHEN r.Ruleset_Date_Effective >= getdate() 
            THEN 'Upcoming'
            ELSE 'Ruleset needs checking' END as [Stage]
            , r.Service_ID, r.Ruleset_ID, max(r.Ruleset_Version) as [Ruleset_Version] -- , r.Ruleset_Date_Effective, r.Ruleset_Date_Inactive
    from {ClusterManagement.Rulesets} r
    where r.Ruleset_Date_Inactive >= getdate() or r.Ruleset_Date_Inactive is null
    group by r.Service_ID, r.Ruleset_ID, r.Ruleset_Date_Effective, r.Ruleset_Date_Inactive
    order by r.Service_ID, [Ruleset_Version], r.Ruleset_ID"""
 

    return query
#-------------------------------------------------------------------------------------------------------------------

def all_live_upcoming_rulesets_with_clusters(ClusterManagement:class_ClusterManagement_dbs):
    query = f"""
        SET NOCOUNT ON

            DROP TABLE IF EXISTS #TT_Live_upcoming_rulesets

            select
            CASE WHEN r.Ruleset_Date_Effective <= getdate() and (r.Ruleset_Date_Inactive >= getdate() or r.Ruleset_Date_Inactive is null) THEN 'Live'
                    WHEN r.Ruleset_Date_Effective >= getdate() 
                    and r.Ruleset_Date_Inactive is null
                    THEN 'Upcoming'
                    ELSE 'Ruleset needs checking' END as [Stage]
                    , r.Service_ID, r.Ruleset_ID, max(r.Ruleset_Version) as [Ruleset_Version] -- , r.Ruleset_Date_Effective, r.Ruleset_Date_Inactive
            into #TT_Live_upcoming_rulesets
            from  {ClusterManagement.Rulesets} r
            where r.Ruleset_Date_Inactive >= getdate() or r.Ruleset_Date_Inactive is null
            group by r.Service_ID, r.Ruleset_ID, Ruleset_Date_Effective, Ruleset_Date_Inactive
            order by r.Service_ID, [Ruleset_Version], r.Ruleset_ID


            SELECT crd2.Stage, crd1.Service_ID, crd1.Ruleset_ID, crd1.Ruleset_Version, crd1.Cluster_ID, crd1.Cluster_Ruleset_Date_Effective, crd1.Cluster_Ruleset_Date_Inactive
            FROM  {ClusterManagement.Cluster_Ruleset} as crd1
            right join #TT_Live_upcoming_rulesets as crd2
            on crd1.Service_ID = crd2.Service_ID and crd1.Ruleset_ID = crd2.Ruleset_ID and crd1.Ruleset_Version = crd2.Ruleset_Version
            ORDER BY Service_ID, Ruleset_ID	
            """
    return query

#-------------------------------------------------------------------------------------------------------------------

def sql_query_static_ruleset_ECC_tbl(Ruleset_ID, Ruleset_version, Service_ID, LocalSNOMEDCT: class_LocalSNOMEDCT_dbs, UKSNOMEDCT: class_UKSNOMEDCT_dbs, ClusterManagement:class_ClusterManagement_dbs):

    
    query = f"""

        -- Step 1: Define vairables
        SET NOCOUNT ON;

            -- ================================================================================
        -- Creation_of_expanded_cluster_list_RULESET_LEVEL
        -- Description:	Creates an expanded cluster list at ruleset-level
        -- ================================================================================

        DECLARE @Ruleset_ID varchar(50),
            @Ruleset_Version decimal(9,1),
            @Service_ID varchar(50),
            @Service_Year varchar(50)
            
        SET @Ruleset_ID = '{Ruleset_ID}'		
        SET @Ruleset_Version = {Ruleset_version}					
        SET @Service_ID = '{Service_ID}'   


        --=================================================================================================================================================
        -- Step 2: drop temporary table where it exists if this code has already been run
        --=================================================================================================================================================
        DROP TABLE IF EXISTS #TT_Clusters

        --=================================================================================================================================================
        -- Step 3: Create a temporary table containing all clusters and refsets used in the selected service. 
        --		   This is drawn from information held in the CRM database.
        --=================================================================================================================================================
        Select cr.Cluster_ID, 
        c.Cluster_Description,
        c.Refset_ID 
        into #TT_Clusters
        from {ClusterManagement.Cluster_Ruleset} cr
        inner join {ClusterManagement.Clusters} c
        on cr.Cluster_ID = c.Cluster_ID
        where cr.Ruleset_ID = @Ruleset_ID and Ruleset_Version = @Ruleset_Version and c.Cluster_Date_Inactive is null

        --ALTER TABLE #TT_Clusters ALTER COLUMN Refset_ID varchar(18) COLLATE Latin1_General_CI_AS

        --=================================================================================================================================================
        -- Step 4: 	Return all fields required for expanded cluster list to be published and shared. 

        --			This includes all the SNOMED concepts included in the refsets used by the selected service, their full descriptions
        --		   	and active status, along with the cluster information captured in Step 3. 
        --		   	This is drawn from information held in the PSU_TRUD tables, and joined to the information from the CRM database.

        --=================================================================================================================================================
        -- SPECIFIED RULESETS ONLY

        Select distinct 'SNOMED CT' as [Coding ID], 
        a.Cluster_ID as [Cluster ID], 
        a.Cluster_Description as [Cluster description],
        r.referencedComponentId as [SNOMED concept ID], 
        d.term as [Code description],
        CASE WHEN a.Refset_ID like '%100023010%' THEN 'PCD Refset' WHEN a.Refset_ID like '%1000001%' THEN 'Drug Refset' ELSE 'Refset' END as [Type_of_Refset],
        '^' + a.Refset_ID as [Cluster_Code_String],
        c.active as [Active status]
        from #TT_Clusters a
        left join {UKSNOMEDCT.SCT_REFSET} r
        on a.Refset_ID = r.refsetId and r.active = 1
        left join {UKSNOMEDCT.SCT_DESCRIPTION} d
        on r.referencedComponentId = d.conceptId and d.active = 1 and d.DescriptionType = 'F'
        left join {UKSNOMEDCT.SCT_CONCEPT} c
        on r.referencedComponentId = c.id
        ORDER BY 6,2

            """
    
    return query


#-------------------------------------------------------------------------------------------------------------------
def sql_query_static_service_ECC_tbl(Ruleset_version, Service_ID,LocalSNOMEDCT: class_LocalSNOMEDCT_dbs, UKSNOMEDCT: class_UKSNOMEDCT_dbs, ClusterManagement:class_ClusterManagement_dbs):

    
    query = f"""

        -- Step 1: Define vairables
        SET NOCOUNT ON;

                    -- ================================================================================
            -- Creation_of_expanded_cluster_list_SERVICE_LEVEL
            -- Description:	Creates an expanded cluster list at service-level
            -- ================================================================================

            -- >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

            /* INSTRUCTIONS */

            -- ! ! ! ! THIS CODE IS FOR GENERATING AN EXPANDED CLUSTER LIST AT SERVICE-LEVEL e.g. QOF, INLIQ etc.  ! ! ! !

            -- ! ! ! ! IF YOU NEED A RULESET-LEVEL e.g. MMR, MenB, EXPANDED CLUSTER LIST PLEASE USE THE Creation_of_expanded_cluster_list_RULESET_level FILE ! ! ! !

            -- 1. Perform a Find and Replace to replace MmmYY with the current code release date.
            -- 2. Replace the 'ENTER SERVICE_ID HERE' part in Step 2 with your required Service_ID.
            -- 3. Replace the 'ENTER RULESET VERSION HERE' part in Step 2 with your required Ruleset_Version.
            -- 4. Run the query.

            -- <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<


            --=================================================================================================================================================
            -- Step 1: drop temporary table where it exists if this code has already been run
            --=================================================================================================================================================
            DROP TABLE IF EXISTS #TT_Clusters

            DECLARE @Ruleset_Version decimal(9,1),
            @Service_ID varchar(50)
            	
            SET @Ruleset_Version = {Ruleset_version}					
            SET @Service_ID = '{Service_ID}' 

            --=================================================================================================================================================
            -- Step 2: Create a temporary table containing all clusters and refsets used in the selected service. 
            --		   This is drawn from information held in the CRM database.
            --=================================================================================================================================================
            Select DISTINCT cr.Cluster_ID, 
                c.Cluster_Description,
                c.Refset_ID 
            into #TT_Clusters
            from {ClusterManagement.Cluster_Ruleset} cr
            inner join {ClusterManagement.Clusters} c
            on cr.Cluster_ID = c.Cluster_ID
            where cr.Service_ID = @Service_ID			
            and Ruleset_Version >= @Ruleset_Version	
            and c.Cluster_Date_Inactive is null
            and cr.[Cluster_Ruleset_Date_Inactive] is null

            --ALTER TABLE #TT_Clusters ALTER COLUMN Refset_ID varchar(18) COLLATE Latin1_General_CI_AS

            --=================================================================================================================================================
            -- Step 3: 	Return all fields required for expanded cluster list to be published and shared. 

            --			This includes all the SNOMED concepts included in the refsets used by the selected service, their full descriptions
            --		   	and active status, along with the cluster information captured in Step 3. 
            --		   	This is drawn from information held in the PSU_TRUD tables, and joined to the information from the CRM database.

            --=================================================================================================================================================
            -- SPECIFIED RULESETS ONLY

            Select distinct 'SNOMED CT' as [Coding ID], 
                a.Cluster_ID as [Cluster ID], 
                a.Cluster_Description as [Cluster description],
                r.referencedComponentId as [SNOMED concept ID], 
                d.term as [Code description],
                CASE WHEN a.Refset_ID like '%100023010%' THEN 'PCD Refset' WHEN a.Refset_ID like '%1000001%' THEN 'Drug Refset' ELSE 'Refset' END as [Type_of_Refset],
                '^' + a.Refset_ID as [Cluster_Code_String],
                c.active as [Active status]
            from #TT_Clusters a
            left join {UKSNOMEDCT.SCT_REFSET} r
            on a.Refset_ID = r.refsetId and r.active = 1
            left join {UKSNOMEDCT.SCT_DESCRIPTION} d
            on r.referencedComponentId = d.conceptId and d.active = 1 and d.DescriptionType = 'F'
            left join {UKSNOMEDCT.SCT_CONCEPT} c
            on r.referencedComponentId = c.id
            ORDER BY 6,2
            """
    
    return query