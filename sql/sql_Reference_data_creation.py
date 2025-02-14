from src.utils.dates import class_Dates
from src.utils.databases import class_UKSNOMEDCT_dbs, class_ClusterManagement_dbs

#10A-------------------------------------------------------------------------------------------------------------------
#Supporting_Products.py
def sql_query_A_Ref_Data_Creation(configDates:class_Dates, UKSNOMEDCT:class_UKSNOMEDCT_dbs, ClusterManagement:class_ClusterManagement_dbs):

    query = f"""
            SET NOCOUNT ON; SET ANSI_WARNINGS OFF;

            --- Instructions

            -- 1) Find and replace all MmmYY for the current release you are working with e.g. Apr21
            -- 2) Find and replace all YYYYMMDD with the date of your PCD refset publication
            -- 3) If you have not yet downloaded a published UK release (i.e. no not have SCT_ tables), then replace SCT_ with the relevant IA0_SCT_ prefix.
            -- 4) Execute the code 

            -- You will see one output - it should be number of clusters and rows within the full GPData table.
        
            --------------------------------------------------------------------------------------------------------------------------------------
            drop table if exists #Refsets_all_clusters
            drop table if exists {UKSNOMEDCT.db}.dbo.GPData_Cluster_refset_1000230_{configDates.YYYYMMDD(configDates.PCDreleaseDate)}
            drop table if exists {UKSNOMEDCT.db}.dbo.GPData_Cluster_refset_1000230_{configDates.YYYYMMDD(configDates.PCDreleaseDate)}_nc
            drop table if exists {UKSNOMEDCT.db}.dbo.GDPPR_Cluster_refset_1000230_{configDates.YYYYMMDD(configDates.PCDreleaseDate)}_nc
            drop table if exists #Refsets_all_clusters
            drop table if exists #retired_active_all_clusters
            ;
            --------------------------------------------------------------------------------------------------------------------------------------
            DECLARE @minus_year AS DATE
            set @minus_year = DATEADD(year, -1, GETDATE())
            
            --Select revelent cluster IDs & detail
            SELECT distinct Cluster_ID, Refset_ID, Cluster_Description, Cluster_Date_Inactive
            ,CASE WHEN Cluster_Category is null THEN 'To be confirmed' ELSE Cluster_Category END as [Cluster_Category]
            into #Refsets_all_clusters
            FROM {ClusterManagement.Clusters}
            where Cluster_ID not in ('GDPPR_COD','GDPPR2YR_COD','POBJEC_COD','POBJECW_COD', 'NAOBJEC_COD', 'NDAOBJEC_COD', 'NACONS_COD', 'NDACONS_COD')----remove the big dedup clusters and those used for patient objections.
            -----------------------------------------------------------
            and (Cluster_ID not like 'DEMENTIA_%' and Cluster_ID like '%_COD%')--Remove Dementia toolkit clusters 
            and Cluster_Description not like '%agreed by the Firearms Working Group based on DVLA driving guidance%' --Remove firearms clusters
            ---------------------------------------------------------------
            and (Cluster_Date_Inactive is null OR Cluster_Date_Inactive >=  @minus_year or Cluster_ID IN ('NDABP_COD')) --includes all active and recently retired (in the last year) clusters --NDABP_COD is inactive as it is no longer maintained but as it is in GDPPR we list it explicitly here.
            and Refset_ID is not null 
            and Refset_ID <> 'N/A'

            --------------------------------------------------------------------------------------------------------------------------------------

            --returns distinct rows that have a recently retired (last 12 months) and an active (null) entry in the Cluster_Date_Inactive col
            SELECT DISTINCT Cluster_ID,
            Refset_ID,
            Cluster_Description,
            CASE WHEN MAX(CASE WHEN Cluster_Date_Inactive IS NULL THEN 1 ELSE 0 END) = 0
                    THEN MAX(Cluster_Date_Inactive) END as [Cluster_Date_Inactive],
            Cluster_Category

            INTO #retired_active_all_clusters
            FROM #Refsets_all_clusters
            GROUP BY Cluster_ID, Refset_ID, Cluster_Description, Cluster_Category
            ORDER BY Cluster_ID, Cluster_Date_Inactive


            ---------------------------------------------------------------
            --Below used to check how many duplicated Cluster_ID rows there are (due to change in Cluster_Description or Cluster_Category)
            ---------------------------------------------------------------
            --SELECT Cluster_ID,
            -- Refset_ID,
            -- COUNT(Cluster_ID)
            --FROM  #retired_active_all_clusters
            --GROUP BY
            -- Cluster_ID,
            -- Refset_ID
            --HAVING COUNT(Cluster_ID) > 1
            --ORDER BY Cluster_ID

            ---------------------------------------------------------------
            --De-duplicate multiple Cluster_ID rows (should return Null Inactive dates over recently retired dates)
            ; WITH TableWithRowID AS
            (
            SELECT ROW_NUMBER() OVER (PARTITION BY [Cluster_ID],[Refset_ID] ORDER BY [Cluster_ID], [Cluster_Date_Inactive]) AS RowID, * 
            from #retired_active_all_clusters
            )
            DELETE o FROM TableWithRowID o WHERE RowID > 1

            ---------------------------------------------------------------

            SELECT DISTINCT 
                CASE WHEN r.Cluster_Category is null	
                    THEN 'To be confirmed' 
                    ELSE r.Cluster_Category 
                    END as [Cluster_Category]
                ,r.Cluster_ID
                ,r.Cluster_Description as [Cluster_Desc]
                , s.active as Active_in_Refset
                , s.effectiveTime Refset_Status_Date
                , s.refsetId as RefsetId
                , d.term as Refset_Description
                , s.referencedComponentId as ConceptId 
                , y.term as ConceptId_Description
                , Case when y.term is null then 'Z' else y.DescriptionType  end as DescriptionType 
                , c.active as Active_Code_Status
                , c.effectiveTime as Code_Status_Date
                , case when s.moduleId = '999000011000230102' then 'PC refset' else 'Refset' end as Type_of_Inclusion
                , case when sc.effectiveTime is not null then sc.active else 0 end as Sensitive_Status
                , case when sc.effectiveTime is not null then sc.effectiveTime else '19000101' end as Sensitive_Status_Date
            INTO {UKSNOMEDCT.db}.dbo.GPData_Cluster_refset_1000230_{configDates.YYYYMMDD(configDates.PCDreleaseDate)}
            FROM {UKSNOMEDCT.SCT_REFSET_prefix} s
            inner join {UKSNOMEDCT.SCT_DESCRIPTION_prefix} d	--match on refset description
                on s.refsetId = d.conceptId 
                and d.active = 1												--active description
                and  d.DescriptionType = 'F'									--fully specified term
            left join {UKSNOMEDCT.SCT_DESCRIPTION_prefix} y			--match on concept code description
                on s.referencedComponentId = y.conceptId 
                and y.active = 1		 										--active description
            left join {UKSNOMEDCT.SCT_CONCEPT_prefix} c				--Match on the code active status
                on c.id = s.referencedComponentId
            left join															--Match on the sensitive code flag using the four refsets 
                    (select distinct [referencedComponentId], active, effectiveTime 
                    from {UKSNOMEDCT.SCT_REFSET_prefix}
                    where refsetId in ('999004371000000100', '999004351000000109','999004381000000103', '999004361000000107')
                    ) sc
                on sc.referencedComponentId = s.referencedComponentId
            inner join #retired_active_all_clusters r									--select only those clusters/refsets we are interested in
                on r.Refset_ID = s.refsetId
            order by Cluster_ID, s.active desc

            --------------------------------------------------------------------------------------------------------------------------------------
            --De-duplicate based on ConceptId and Cluster_ID; description type preference selected by way of 'order by' statement (F>P>S>Null)
            ; WITH TableBWithRowID AS
            (
            SELECT ROW_NUMBER() OVER (PARTITION BY [ConceptId],[Cluster_ID] ORDER BY [ConceptId],[Active_in_Refset] desc,case when [DescriptionType] is null then 'z' else [DescriptionType] end, ConceptId_Description ) AS RowID, * 
            from {UKSNOMEDCT.db}.dbo.GPData_Cluster_refset_1000230_{configDates.YYYYMMDD(configDates.PCDreleaseDate)}
            )
            DELETE o FROM TableBWithRowID o WHERE RowID > 1

            --------------------------------------------------------------------------------------------------------------------------------------
            --- CHECKS:

            select Count (distinct Cluster_ID) as [Number of clusters], Count (Cluster_ID) as [Number of rows]
            from {UKSNOMEDCT.db}.dbo.GPData_Cluster_refset_1000230_{configDates.YYYYMMDD(configDates.PCDreleaseDate)}       
        """
    return query
#-------------------------------------------------------------------------------------------------------------------

##-------------------------------------------------------------------------------------------------------------------
#Supporting_Products.py
def sql_query_B_Ref_Data_Creation(configDates:class_Dates, UKSNOMEDCT:class_UKSNOMEDCT_dbs, ClusterManagement: class_ClusterManagement_dbs):

    query = f"""
            SET NOCOUNT ON; SET ANSI_WARNINGS OFF;

            -- You should see one output -  a blank table.
            -- If the table contains records you will need to investigate and correct as necessary.
            -- Two tables with a "_nc" suffix will be created in your database and need exporting as csvs.

            --Combine other checks:
            --this should return no records but needs checking
            Select * from (
                            --check there are no rows with a null concept description
                            select 'Records with null code description' as [Table purpose], * 
                            from {UKSNOMEDCT.db}.dbo.GPData_Cluster_refset_1000230_{configDates.YYYYMMDD(configDates.PCDreleaseDate)}
                            where ConceptId_Description is null 
                            
                            UNION ALL
                            --check there are no more special characters
                            select 'Descriptions with special characters' as [Table purpose], * 
                            from  {UKSNOMEDCT.db}.dbo.GPData_Cluster_refset_1000230_{configDates.YYYYMMDD(configDates.PCDreleaseDate)} 
                            WHERE ConceptId_Description like '%Ã%' or Refset_Description like '%Ã%'
                            
                            UNION ALL
                            --check there NULL cluster categories
                            select 'Cluster category is null' as [Table purpose], * 
                            from  {UKSNOMEDCT.db}.dbo.GPData_Cluster_refset_1000230_{configDates.YYYYMMDD(configDates.PCDreleaseDate)} 
                            WHERE Cluster_Category = 'NULL' or Cluster_Category is null

                            ) a ;

            --------------------------------------------------------------------------------------------------------------------------------------

            --Make 2 further tables with _nc (no commas) suffix in the old style 
            --(i.e. no speech marks, but remove all commas within descriptions so it doesn't cause havoc when importing a csv)
            select Cluster_Category
            , Cluster_ID
            , Cluster_Desc
            , Active_in_Refset
            , Refset_Status_Date
            , RefsetId
            , Refset_Description
            , ConceptId 
            , ConceptId_Description
            , Active_Code_Status
            , Code_Status_Date
            , Type_of_Inclusion
            , Sensitive_Status
            , Sensitive_Status_Date 
            into {UKSNOMEDCT.db}.dbo.GPData_Cluster_refset_1000230_{configDates.YYYYMMDD(configDates.PCDreleaseDate)}_nc
            from {UKSNOMEDCT.db}.dbo.GPData_Cluster_refset_1000230_{configDates.YYYYMMDD(configDates.PCDreleaseDate)}
            order by 1

            --Find and replace commas in 4 description fields
            UPDATE {UKSNOMEDCT.db}.dbo.GPData_Cluster_refset_1000230_{configDates.YYYYMMDD(configDates.PCDreleaseDate)}_nc
            SET Cluster_Category = REPLACE(Cluster_Category, ',','') 
            WHERE Cluster_Category like '%,%';

            UPDATE {UKSNOMEDCT.db}.dbo.GPData_Cluster_refset_1000230_{configDates.YYYYMMDD(configDates.PCDreleaseDate)}_nc
            SET Cluster_Desc = REPLACE(Cluster_Desc, ',','') 
            WHERE Cluster_Desc like '%,%';

            UPDATE {UKSNOMEDCT.db}.dbo.GPData_Cluster_refset_1000230_{configDates.YYYYMMDD(configDates.PCDreleaseDate)}_nc
            SET Refset_Description = REPLACE(Refset_Description, ',','') 
            WHERE Refset_Description like '%,%';

            UPDATE {UKSNOMEDCT.db}.dbo.GPData_Cluster_refset_1000230_{configDates.YYYYMMDD(configDates.PCDreleaseDate)}_nc
            SET ConceptId_Description = REPLACE(ConceptId_Description, ',','') 
            WHERE ConceptId_Description like '%,%';

            UPDATE {UKSNOMEDCT.db}.dbo.GPData_Cluster_refset_1000230_{configDates.YYYYMMDD(configDates.PCDreleaseDate)}_nc
            SET ConceptId_Description = REPLACE(ConceptId_Description, '–','-') 
            WHERE ConceptId_Description like '%,%';

            --Now from this create the GDPPR refset file
            select distinct * 
            into {UKSNOMEDCT.db}.dbo.GDPPR_Cluster_refset_1000230_{configDates.YYYYMMDD(configDates.PCDreleaseDate)}_nc
            from {UKSNOMEDCT.db}.dbo.GPData_Cluster_refset_1000230_{configDates.YYYYMMDD(configDates.PCDreleaseDate)}_nc
            where Cluster_ID in (select Cluster_ID from {ClusterManagement.db}.dbo.Cluster_Ruleset 
                                where Ruleset_ID = 'GDPPR' 
                                and Cluster_Ruleset_Date_Inactive is null)
            order by 1

            --***********************************************************************************

            """
    return query
#-------------------------------------------------------------------------------------------------------------------
