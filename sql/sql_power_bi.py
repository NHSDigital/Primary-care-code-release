from src.utils.databases import class_UKSNOMEDCT_dbs, class_LocalSNOMEDCT_dbs, class_ClusterManagement_dbs

def sql_query_ServiceRulesetIDs(ClusterManagement:class_ClusterManagement_dbs):
    """
    Takes the Cluster Management database.
    Generates a query which returns all the published rulesets.
    Returns a SQL query string.
    """
    query = f"""
            -- Query A
            SELECT DISTINCT r.[Service_ID], r.[Ruleset_ID]
            FROM {ClusterManagement.Rulesets} r
            INNER JOIN {ClusterManagement.Ruleset_Published} as p
            on r.Ruleset_ID = p.Ruleset_ID COLLATE Latin1_General_CI_AS 
            and r.Ruleset_Version = p.Ruleset_Version
            and r.Service_ID = p.Service_ID COLLATE Latin1_General_CI_AS 
            where (p.Ruleset_Publication_Inactive is null or p.Ruleset_Publication_Inactive >= (select getdate())) -- This ensures that include service types containing rulesets where the ruleset publication inactive date has not yet been set, or where the ruleset publication inactive date has not yet been reached (based on date of running this query)
            and p.[Ruleset_Publication_Effective] <= (select getdate())
            ORDER BY 1,2
            """
    return query


def sql_query_ClusterRuleset(ClusterManagement:class_ClusterManagement_dbs):
    """
    Takes the Cluster Management database.
    Generates a query which returns all the clusters used by the published rulesets.
    Returns a SQL query string.
    """
    query = f"""
             -- Query B
            SELECT DISTINCT cr.Service_ID, cr.Ruleset_ID, cr.Ruleset_Version, cr.Cluster_ID
            FROM {ClusterManagement.Cluster_Ruleset} as cr
            INNER JOIN	{ClusterManagement.Ruleset_Published} as rp -- This joins onto the Ruleset_published table
                ON cr.Service_ID = rp.Service_ID COLLATE Latin1_General_CI_AS 
                AND cr.Ruleset_ID = rp.Ruleset_ID COLLATE Latin1_General_CI_AS 
                AND cr.Ruleset_Version = rp.Ruleset_Version
            WHERE (rp.Ruleset_Publication_Inactive IS NULL OR rp.Ruleset_Publication_Inactive >= (select getdate()))
            AND rp.[Ruleset_Publication_Effective] <= (select getdate())
            ORDER BY 1,2
            """
    return query

def sql_query_Clusters(ClusterManagement:class_ClusterManagement_dbs):
    """
    Takes the Cluster Management database.
    Generates a query which returns the clusters and refset IDs used by the published rulesets.
    Returns a SQL query string.
    """
    query = f"""
            -- Query C
            SELECT DISTINCT cl.Cluster_ID, cl.Cluster_Description, cl.Refset_ID
            FROM {ClusterManagement.Clusters} as cl
            INNER JOIN {ClusterManagement.Cluster_Ruleset} as cr
                ON cl.Cluster_ID = cr.Cluster_ID
            INNER JOIN {ClusterManagement.Ruleset_Published} as rp -- This joins onto the Ruleset_published table
                ON cr.Service_ID = rp.Service_ID COLLATE Latin1_General_CI_AS 
                AND cr.Ruleset_ID = rp.Ruleset_ID COLLATE Latin1_General_CI_AS 
                AND cr.Ruleset_Version = rp.Ruleset_Version 
            WHERE cl.Cluster_Date_Inactive IS NULL
            AND (rp.Ruleset_Publication_Inactive IS NULL OR rp.Ruleset_Publication_Inactive > (select getdate()))
            AND rp.[Ruleset_Publication_Effective] <= (select getdate())
            """
    return query

def sql_query_OutputRuleset(ClusterManagement:class_ClusterManagement_dbs):
    """
    Takes the Cluster Management database.
    Generates a query which returns all the outputs used by the published rulesets.
    Returns a SQL query string.
    """
    query = f"""
            -- Query D
            SELECT DISTINCT orl.Service_ID, orl.Ruleset_ID, orl.Ruleset_Version, orl.Output_ID, orl.Output_Version
            FROM {ClusterManagement.Output_Ruleset} as orl
            INNER JOIN {ClusterManagement.Ruleset_Published} as rp -- This joins onto the Ruleset_published table
                ON orl.Service_ID = rp.Service_ID COLLATE Latin1_General_CI_AS 
                AND orl.Ruleset_ID = rp.Ruleset_ID COLLATE Latin1_General_CI_AS 
                AND orl.Ruleset_Version = rp.Ruleset_Version
            WHERE (rp.Ruleset_Publication_Inactive IS NULL OR rp.Ruleset_Publication_Inactive > (select getdate()))
            AND rp.[Ruleset_Publication_Effective] <= (select getdate())           
            ORDER BY 1,2,4
            """
    return query


def sql_query_Outputs(ClusterManagement:class_ClusterManagement_dbs):
    """
    Takes the Cluster Management database.
    Generates a query which returns all the outputs, descriptions and types for those outputs used by the published rulesets.
    Returns a SQL query string.
    """
    query = f"""
             -- Query E
            SELECT DISTINCT o.Output_ID, o.Output_Version, o.Output_Description, o.Output_Type
            FROM {ClusterManagement.Outputs} as o
            INNER JOIN {ClusterManagement.Output_Ruleset} as orl
                ON o.Output_ID = orl.Output_ID
                AND o.Output_Version = orl.Output_Version
            INNER JOIN {ClusterManagement.Ruleset_Published} as rp -- This joins onto the Ruleset_published table
                ON orl.Service_ID = rp.Service_ID COLLATE Latin1_General_CI_AS 
                AND orl.Ruleset_ID = rp.Ruleset_ID COLLATE Latin1_General_CI_AS 
                AND orl.Ruleset_Version = rp.Ruleset_Version
            WHERE (rp.Ruleset_Publication_Inactive IS NULL OR rp.Ruleset_Publication_Inactive > (select getdate()))
            AND rp.[Ruleset_Publication_Effective] <= (select getdate())           
            ORDER BY 1,2
            """
    return query

def sql_query_PopulationRuleset(ClusterManagement:class_ClusterManagement_dbs):
    """
    Takes the Cluster Management database.
    Generates a query which returns all the populations used by the published rulesets.
    Returns a SQL query string.
    """
    query = f"""
          -- Query F
            SELECT pr.Service_ID, pr.Ruleset_ID, pr.Ruleset_Version, pr.Population_ID, pr.Population_Version
            FROM {ClusterManagement.Population_Ruleset} as pr
            INNER JOIN {ClusterManagement.Ruleset_Published} as rp -- This joins onto the Ruleset_published table
                ON pr.Service_ID = rp.Service_ID COLLATE Latin1_General_CI_AS 
                AND pr.Ruleset_ID = rp.Ruleset_ID COLLATE Latin1_General_CI_AS 
                AND pr.Ruleset_Version = rp.Ruleset_Version
            WHERE (rp.Ruleset_Publication_Inactive IS NULL OR rp.Ruleset_Publication_Inactive > (select getdate()))
            AND rp.[Ruleset_Publication_Effective] <= (select getdate())           
            ORDER BY 1,2,4
            """
    return query

def sql_query_Populations(ClusterManagement:class_ClusterManagement_dbs):
    """
    Takes the Cluster Management database.
    Generates a query which returns all the populations, descriptions and types for those populations used by the published rulesets
    Returns a SQL query string.
    """
    query = f"""
            -- Query G
            SELECT DISTINCT p.Population_ID, p.Population_Description, p.Population_Version, p.Population_Type
            FROM {ClusterManagement.Populations} as p
            INNER JOIN {ClusterManagement.Population_Ruleset} as pr
                ON p.Population_ID = pr.Population_ID
                AND p.Population_Version = pr.Population_Version
            INNER JOIN {ClusterManagement.Ruleset_Published} as rp -- This joins onto the Ruleset_published table
                ON pr.Service_ID = rp.Service_ID COLLATE Latin1_General_CI_AS 
                AND pr.Ruleset_ID = rp.Ruleset_ID COLLATE Latin1_General_CI_AS 
                AND pr.Ruleset_Version = rp.Ruleset_Version
            WHERE (rp.Ruleset_Publication_Inactive IS NULL OR rp.Ruleset_Publication_Inactive > (select getdate()))
            AND rp.[Ruleset_Publication_Effective] <= (select getdate())           
            ORDER BY 1,3
            """
    return query

def sql_query_ClusterPopulation(ClusterManagement:class_ClusterManagement_dbs):
    """
    Takes the Cluster Management database.
    Generates a query which returns all the clusters, and the populations they are linked to, in the published rulesets.
    Returns a SQL query string.
    """
    query = f"""
            -- Query H
            SELECT DISTINCT cp.Population_ID, cp.Population_Version, cp.Cluster_ID
            FROM {ClusterManagement.Cluster_Population} as cp
            INNER JOIN {ClusterManagement.Population_Ruleset} as pr
                ON cp.Population_ID = pr.Population_ID
                AND cp.Population_Version = pr.Population_Version
            INNER JOIN {ClusterManagement.Ruleset_Published} as rp -- This joins onto the Ruleset_published table
                ON pr.Service_ID = rp.Service_ID COLLATE Latin1_General_CI_AS 
                AND pr.Ruleset_ID = rp.Ruleset_ID COLLATE Latin1_General_CI_AS 
                AND pr.Ruleset_Version = rp.Ruleset_Version
            INNER JOIN {ClusterManagement.Clusters} as cl
                ON cp.Cluster_ID = cl.Cluster_ID
            WHERE (rp.Ruleset_Publication_Inactive IS NULL OR rp.Ruleset_Publication_Inactive > (select getdate()))
            AND rp.[Ruleset_Publication_Effective] <= (select getdate()) 
            AND cl.Cluster_Date_Inactive IS NULL
           
            ORDER BY 1,2,3
            """ 
    return query

def sql_query_OutputPopulation(ClusterManagement:class_ClusterManagement_dbs):
    """
    Takes the Cluster Management database.
    Generates a query which returns all the populations, and the outputs they are linked to, in the published rulesets.
    Returns a SQL query string.
    """
    query = f"""
            -- Query I
            SELECT DISTINCT op.Population_ID, op.Population_Version, op.Output_ID, op.Output_Version
            FROM {ClusterManagement.Output_Population} as op
            INNER JOIN (-- Query F
                SELECT DISTINCT pr.Population_ID, pr.Population_Version
                FROM {ClusterManagement.Population_Ruleset} as pr
                INNER JOIN {ClusterManagement.Ruleset_Published} as rp -- This joins onto the Ruleset_published table
                    ON pr.Service_ID = rp.Service_ID COLLATE Latin1_General_CI_AS 
                    AND pr.Ruleset_ID = rp.Ruleset_ID COLLATE Latin1_General_CI_AS 
                    AND pr.Ruleset_Version = rp.Ruleset_Version
                WHERE (rp.Ruleset_Publication_Inactive IS NULL OR rp.Ruleset_Publication_Inactive > (select getdate()))
                AND rp.[Ruleset_Publication_Effective] <= (select getdate()) 
                ) F
           ON op.Population_ID = F.Population_ID
           AND op.Population_Version = F.Population_Version
           INNER JOIN (--Query E
                SELECT DISTINCT orl.Output_ID, orl.Output_Version
                FROM {ClusterManagement.Output_Ruleset} as orl
                INNER JOIN {ClusterManagement.Ruleset_Published} as rp -- This joins onto the Ruleset_published table
                    ON orl.Service_ID = rp.Service_ID COLLATE Latin1_General_CI_AS 
                    AND orl.Ruleset_ID = rp.Ruleset_ID COLLATE Latin1_General_CI_AS 
                    AND orl.Ruleset_Version = rp.Ruleset_Version
                WHERE (rp.Ruleset_Publication_Inactive IS NULL OR rp.Ruleset_Publication_Inactive > (select getdate()))
                AND rp.[Ruleset_Publication_Effective] <= (select getdate())    
                ) E
            ON op.Output_ID = E.Output_ID
            AND op.Output_Version = E.Output_Version
            ORDER BY 1,2,3,4
            """ 
    return query

def sql_query_ClusterOutput(ClusterManagement:class_ClusterManagement_dbs):
    """
    Takes the Cluster Management database.
    Generates a query which returns all the outputs, and the clusters they are linked to, in the published rulesets.
    Returns a SQL query string.
    """
    query = f"""
            -- Query J
            SELECT DISTINCT co.[Output_ID], co.[Output_Version], co.[Cluster_ID]
            FROM {ClusterManagement.Cluster_Output} as co
            INNER JOIN {ClusterManagement.Output_Ruleset} as orl
                ON co.Output_ID = orl.Output_ID
                AND co.Output_Version = orl.Output_Version
            INNER JOIN {ClusterManagement.Ruleset_Published} as rp -- This joins onto the Ruleset_published table
                ON orl.Service_ID = rp.Service_ID COLLATE Latin1_General_CI_AS 
                AND orl.Ruleset_ID = rp.Ruleset_ID COLLATE Latin1_General_CI_AS 
                AND orl.Ruleset_Version = rp.Ruleset_Version
            INNER JOIN {ClusterManagement.Clusters} as cl
                ON co.Cluster_ID = cl.Cluster_ID
            WHERE (rp.Ruleset_Publication_Inactive IS NULL OR rp.Ruleset_Publication_Inactive > (select getdate()))
            AND rp.[Ruleset_Publication_Effective] <= (select getdate()) 
            AND cl.Cluster_Date_Inactive IS NULL
           
            ORDER BY 1,2,3
            """
    return query

def sql_query_ClusterRefsetContent(ClusterManagement:class_ClusterManagement_dbs, LocalSNOMEDCT:class_LocalSNOMEDCT_dbs, UKSNOMEDCT:class_UKSNOMEDCT_dbs):
    """
    Takes the Cluster Management database, Local SNOMED CT database and UK SNOMED CT database.
    Generates a query which returns the cluster/refset details along with their SNOMED codes for all the clusters used in the published rulesets.
    Returns a SQL query string.
    """
    query = f"""
            -- Query K
            SELECT DISTINCT nq12.[Cluster_ID], nq11.[refsetId] as [PCD Refset ID], nq11.[referencedComponentId] as [SNOMED code], sd.term as [SNOMED code description]
            FROM
                (
                SELECT [id], [refsetId], [referencedComponentId], MAX([effectiveTime]) as [MAXeffectiveTime]  
                FROM {LocalSNOMEDCT.Refsets}
				WHERE effectiveTime <> '99999999'
                GROUP BY [id], [refsetId], [referencedComponentId]
                ) as nq11																	-- This subquery finds the most recent combination of id, refset ID and SNOMED code from the Refsets_Full_Ongoing table
            INNER JOIN {LocalSNOMEDCT.Refsets} as rfo
            ON 
            nq11.MAXeffectiveTime = rfo.effectiveTime
            AND nq11.id = rfo.id
            LEFT JOIN {UKSNOMEDCT.SCT_DESCRIPTION} as sd 
            ON nq11.referencedComponentId = sd.conceptId
            AND sd.DescriptionType = 'F'					-- Returns only the fully specified name (F) for the SNOMED code description.
            AND sd.[active] = 1							-- Returns only the active (1) SNOMED code descriptions.
            INNER JOIN -- This nested query is the same as Query C
                        (	SELECT DISTINCT cl.Cluster_ID, cl.Cluster_Description, cl.Refset_ID
                            FROM {ClusterManagement.Clusters} as cl
                            INNER JOIN -- This nested query is the same as Query B
                                (	SELECT DISTINCT cr.Service_ID, cr.Ruleset_ID, cr.Ruleset_Version, cr.Cluster_ID
                                    FROM {ClusterManagement.Cluster_Ruleset} as cr
                                    INNER JOIN	-- The nested query is the same as Query A 
                                    (	SELECT DISTINCT r.[Service_ID], r.[Ruleset_ID], r.[Ruleset_Version]
                                        FROM {ClusterManagement.Rulesets} r
                                        INNER JOIN {ClusterManagement.Ruleset_Published} as rp -- This joins onto the Ruleset_published table
                                        ON r.Service_ID = rp.Service_ID COLLATE Latin1_General_CI_AS 
                                        AND r.Ruleset_ID = rp.Ruleset_ID COLLATE Latin1_General_CI_AS 
                                        AND r.Ruleset_Version = rp.Ruleset_Version
                                        WHERE (rp.Ruleset_Publication_Inactive IS NULL OR rp.Ruleset_Publication_Inactive > (select getdate()))
                                        AND rp.[Ruleset_Publication_Effective] <= (select getdate())
                                    ) as nq1
                                ON nq1.Service_ID = cr.Service_ID COLLATE Latin1_General_CI_AS
                                AND nq1.Ruleset_ID = cr.Ruleset_ID COLLATE Latin1_General_CI_AS
                                AND nq1.Ruleset_Version = cr.Ruleset_Version
                            ) as nq2
                        ON cl.Cluster_ID = nq2.Cluster_ID
                        WHERE cl.Cluster_Date_Inactive IS NULL
                        AND (cl.[Cluster_ID] NOT LIKE 'DEMENTIA_%' AND cl.[Cluster_ID] LIKE '%_COD%')		-- Prevents Dementia service (that is not ours) clusters being included
                        AND cl.[Cluster_Description] NOT LIKE '%agreed by the Firearms Working Group based on DVLA driving guidance%' -- Prevents firearms clusters being included
                    ) as nq12
            ON nq11.refsetId = nq12.[Refset_ID]
            WHERE rfo.active = 1						-- Returns only active (1) refset content.
            AND nq11.MAXeffectiveTime <> '99999999'		-- Returns only refset content that has been published as anything at 99999999 has not yet been released

            UNION -- Joins above table to the results of the one below
            -- Query to return the Cluster_ID and Refset_ID of all drug clusters.  SNOMED code and SNOMED code description columns added to indicate these clusters are drug ones and have no content showing as the PCD
            -- do not maintain these ones.

            (SELECT DISTINCT clu.[Cluster_ID], clu.[Refset_ID], 
            'Check the SNOMED browser for refset content' as [SNOMED code],
            'Refset not managed or maintained by the PCD team'as [SNOMED code description]
            FROM {ClusterManagement.Clusters} as clu
            INNER JOIN {ClusterManagement.Cluster_Ruleset} as cru
                    ON clu.Cluster_ID = cru.Cluster_ID
                    INNER JOIN {ClusterManagement.Ruleset_Published} as p
                    ON cru.Ruleset_ID = p.Ruleset_ID COLLATE Latin1_General_CI_AS 
                    AND cru.Ruleset_Version = p.Ruleset_Version
                    AND cru.Service_ID = p.Service_ID COLLATE Latin1_General_CI_AS
                    WHERE clu.[Cluster_Date_Inactive] IS NULL
                    AND clu.Refset_ID NOT LIKE '%100023010%'
                    AND ((p.Ruleset_Publication_Inactive IS NULL 
                    OR p.Ruleset_Publication_Inactive > (SELECT getdate())))
                    AND p.[Ruleset_Publication_Effective] <= (select getdate())     
                    AND (clu.Cluster_ID NOT LIKE 'DEMENTIA_%' AND clu.Cluster_ID LIKE '%_COD%')		-- Prevents Dementia service (that is not ours) clusters being included
                    AND clu.Cluster_Description NOT LIKE '%agreed by the Firearms Working Group based on DVLA driving guidance%')
            ORDER BY 1
            """
    return query

def sql_query_PCDRefsetContentFile(ClusterManagement:class_ClusterManagement_dbs, LocalSNOMEDCT:class_LocalSNOMEDCT_dbs, UKSNOMEDCT:class_UKSNOMEDCT_dbs):
    """
    Takes the Cluster Management database, Local SNOMED CT database and UK SNOMED CT database.
    Generates a query which returns the cluster/refset details along with their SNOMED codes for all the clusters used in the published rulesets to later export as a text file.
    Returns a SQL query string.
    """
    query = f"""
            -- PCD_Refset_Content .txt file
            SET NOCOUNT ON

            --Find all clusters used in published rulesets
            DROP TABLE IF EXISTS #TT2
            SELECT DISTINCT a.[Cluster_ID], a.[Cluster_Description], a.[Refset_ID], b.[Service_ID], b.[Ruleset_ID]
            INTO #TT2
            FROM {ClusterManagement.Clusters} as a
            INNER JOIN {ClusterManagement.Cluster_Ruleset} as b
                ON a.Cluster_ID = b.Cluster_ID
            INNER JOIN {ClusterManagement.Ruleset_Published} as rp -- This joins onto the Ruleset_published table
                ON b.Service_ID = rp.Service_ID COLLATE Latin1_General_CI_AS 
                AND b.Ruleset_ID = rp.Ruleset_ID COLLATE Latin1_General_CI_AS 
                AND b.Ruleset_Version = rp.Ruleset_Version
            WHERE (rp.Ruleset_Publication_Inactive IS NULL OR rp.Ruleset_Publication_Inactive > (select getdate()))
                AND rp.[Ruleset_Publication_Effective] <= (select getdate())
                AND a.[Cluster_Date_Inactive] IS NULL
                AND a.Refset_ID LIKE '%100023010%'		-- ensure only refsets managed by pcd are returned


            -- Use the temporary table created above to then creates a further temporary table of all the clusters, refset IDs and which active services and rulesets they appear in.
            DROP TABLE IF EXISTS #TT_Rulesets
            SELECT a.Cluster_ID, a.Cluster_Description, a.Refset_ID,
                    [Rulesets] = STUFF( ( SELECT DISTINCT ' | ' + Service_ID + ' ' + Ruleset_ID
                                        FROM #TT2 b WHERE a.Cluster_ID = b.Cluster_ID
                                        FOR XML PATH('') 
                                        ), 1, 3, '') 
            INTO #TT_Rulesets
			FROM #TT2 a
            GROUP BY a.Cluster_ID, a.Cluster_Description, a.Refset_ID          

            -- Creates the txt file that accompanies the Power BI report.  This needs to be exported to the Shared drive 
            SELECT DISTINCT 
            e.Cluster_ID
            ,e.Cluster_Description as [Cluster_description]
            ,b.[referencedComponentId] as [SNOMED_code]
            ,c.term as [SNOMED_code_description]
            ,b.[refsetId] as [PCD Refset ID]
            ,CAST (e.Rulesets as varchar(MAX)) as [Service_and_Ruleset] -- CAST used to change the Rulesets column to the correct format for import and export
            FROM
                (
                SELECT [id], [refsetId], [referencedComponentId], MAX([effectiveTime]) as [MAXeffectiveTime]  
                FROM {LocalSNOMEDCT.Refsets}		
                WHERE effectiveTime < '99999999'							   	
                GROUP BY [id], [refsetId], [referencedComponentId]
                ) as a										-- This subquery finds the most recent combination of id, refset ID and SNOMED code from the {LocalSNOMEDCT.Refsets} table
            INNER JOIN {LocalSNOMEDCT.Refsets} as b
                ON a.MAXeffectiveTime = b.effectiveTime
                AND a.id = b.id
            LEFT JOIN {UKSNOMEDCT.SCT_DESCRIPTION} as c  -- Update to the relevant code release date
                ON a.referencedComponentId = c.conceptId
                AND c.DescriptionType = 'F'
                AND c.[active] = 1
            INNER JOIN #TT_Rulesets as e
                ON b.refsetId = e.Refset_ID COLLATE Latin1_General_CI_AS
            WHERE b.active = 1 
            """
    return query

def sql_query_OutputDescFile(ClusterManagement:class_ClusterManagement_dbs):
    """
    Takes the Cluster Management database.
    Generates a query which returns the details, including type, for all the outputs/populations used in the published rulesets.
    Returns a SQL query string.
    """
    query = f"""         
            -- ================================================================================================================================================
            -- Output_LookUp .txt file

            -- Produces an Output_LookUp txt file which uses four queries from the SQL used to produce the Excel files for the Power BI report (Query D, 
            -- Query E, Query F and Query G).
            -- ================================================================================================================================================

            SET NOCOUNT ON  

            -- ============
            -- Outputs
            -- ============

            SELECT DISTINCT orl.Service_ID, orl.Ruleset_ID, orl.Ruleset_Version, orl.Output_ID, orl.Output_Version
            INTO #TT_OR
            FROM {ClusterManagement.Output_Ruleset} as orl
            INNER JOIN {ClusterManagement.Ruleset_Published} as rp -- This joins onto the Ruleset_published table
                ON orl.Service_ID = rp.Service_ID COLLATE Latin1_General_CI_AS 
                AND orl.Ruleset_ID = rp.Ruleset_ID COLLATE Latin1_General_CI_AS 
                AND orl.Ruleset_Version = rp.Ruleset_Version
            WHERE (rp.Ruleset_Publication_Inactive IS NULL OR rp.Ruleset_Publication_Inactive > (select getdate()))
            AND rp.[Ruleset_Publication_Effective] <= (select getdate())                
            ORDER BY 1,2,3,4

            SELECT DISTINCT o.Output_ID, o.Output_Version, o.Output_Description, o.Output_Type, Service_ID, Ruleset_ID
            INTO #TT_O
            FROM {ClusterManagement.Outputs} as o
            INNER JOIN #TT_OR nq4
            ON o.Output_ID = nq4.Output_ID
            AND o.Output_Version = nq4.Output_Version
            ORDER BY 1,2

            -- ============
            -- Populations
            -- ============

            SELECT pr.Service_ID, pr.Ruleset_ID, pr.Ruleset_Version, pr.Population_ID, pr.Population_Version
            INTO #TT_PR
            FROM {ClusterManagement.Population_Ruleset} as pr
            INNER JOIN  {ClusterManagement.Ruleset_Published} as rp -- This joins onto the Ruleset_published table
                ON pr.Service_ID = rp.Service_ID COLLATE Latin1_General_CI_AS 
                AND pr.Ruleset_ID = rp.Ruleset_ID COLLATE Latin1_General_CI_AS 
                AND pr.Ruleset_Version = rp.Ruleset_Version
            WHERE (rp.Ruleset_Publication_Inactive IS NULL OR rp.Ruleset_Publication_Inactive > (select getdate()))
                AND rp.[Ruleset_Publication_Effective] <= (select getdate())  
            ORDER BY 1,2,3,4

            SELECT DISTINCT p.Population_ID, p.Population_Description, p.Population_Version, p.Population_Type, Service_ID, Ruleset_ID
            INTO #TT_P
            FROM {ClusterManagement.Populations} as p
            INNER JOIN #TT_PR nq6
                ON p.Population_ID = nq6.Population_ID
                AND p.Population_Version = nq6.Population_Version
            ORDER BY 1,3

            -- ====================================================================================================
            -- Joins the two temporary tables above to create a table showing all the outputs and populations
            -- where the table has a final column indicating the Type of either 'O' for Output or 
            -- 'P' for Population.
            -- ====================================================================================================

            SELECT Service_ID, Ruleset_ID, Output_ID, Output_Description, [Type] = 'O' FROM #TT_O 
            UNION
            SELECT Service_ID, Ruleset_ID, Population_ID, Population_Description, [Type] = 'P' FROM #TT_P 
            ORDER BY 1,2,3
            """
    return query

def sql_query_RefsetByOutputFile(ClusterManagement:class_ClusterManagement_dbs, LocalSNOMEDCT:class_LocalSNOMEDCT_dbs, UKSNOMEDCT:class_UKSNOMEDCT_dbs):
    """
    Takes the Cluster Management database, Local SNOMED CT database and UK SNOMED CT database.
    Generates a query which returns refset content by output for all the outputs used in the published rulesets.
    Returns a SQL query string.
    """
    query = f"""
            SET NOCOUNT ON

            -- ============================
            -- Outputs
            -- ============================

            -- Outputs and which clusters they use
            SELECT DISTINCT co.[Output_ID], co.[Output_Version], co.[Cluster_ID]
            INTO #TT_Cluster_Outputs
            FROM {ClusterManagement.Cluster_Output} as co
            INNER JOIN {ClusterManagement.Output_Ruleset} as orl
                ON co.[Output_ID] = orl.[Output_ID]
                AND co.[Output_Version] = orl.[Output_Version]            
            INNER JOIN	{ClusterManagement.Ruleset_Published} as rp -- This joins onto the Ruleset_published table
                ON orl.Service_ID = rp.Service_ID COLLATE Latin1_General_CI_AS 
                AND orl.Ruleset_ID = rp.Ruleset_ID COLLATE Latin1_General_CI_AS 
                AND orl.Ruleset_Version = rp.Ruleset_Version
            WHERE (rp.Ruleset_Publication_Inactive IS NULL OR rp.Ruleset_Publication_Inactive > (select getdate()))
            AND rp.[Ruleset_Publication_Effective] <= (select getdate())  
                                      
            -- ============================
            -- Populations
            -- ============================

            -- Populations and which clusters they use
            SELECT DISTINCT cp.Population_ID, cp.Population_Version, cp.[Cluster_ID]
            INTO #TT_Cluster_Populations
            FROM {ClusterManagement.Cluster_Population} as cp
            INNER JOIN {ClusterManagement.Population_Ruleset} as pr
                ON cp.Population_ID = pr.Population_ID
                AND cp.Population_Version = pr.Population_Version                                            
            INNER JOIN {ClusterManagement.Ruleset_Published} as rp -- This joins onto the Ruleset_published table
                ON pr.Service_ID = rp.Service_ID COLLATE Latin1_General_CI_AS 
                AND pr.Ruleset_ID = rp.Ruleset_ID COLLATE Latin1_General_CI_AS 
                AND pr.Ruleset_Version = rp.Ruleset_Version
            WHERE (rp.Ruleset_Publication_Inactive IS NULL OR rp.Ruleset_Publication_Inactive > (select getdate()))
            AND rp.[Ruleset_Publication_Effective] <= (select getdate())  

            -- ============================
            -- Clusters
            -- ============================
            -- Clusters and their cluster descriptions
            SELECT DISTINCT cl.Cluster_ID, cl.Cluster_Description, cl.Refset_ID
            INTO #TT_Cluster_Descriptions
            FROM {ClusterManagement.Clusters} as cl
            WHERE cl.Cluster_Date_Inactive IS NULL
                                               
            -- ============================================
            -- Outputs and Populations UNION
            -- ============================================

            -- Creating a single table for the output and population tables
                SELECT DISTINCT TTCO.Output_ID, TTCO.Cluster_ID, TTCD.Cluster_Description
                INTO #TT_Output_Pop_Clusters
                FROM #TT_Cluster_Outputs as TTCO
                INNER JOIN #TT_Cluster_Descriptions as TTCD
                ON TTCO.Cluster_ID = TTCD.Cluster_ID
            UNION
            -- Adding the cluster descriptions to the populations table
                SELECT DISTINCT TTCP.Population_ID, TTCP.Cluster_ID, TTCD.Cluster_Description
                FROM #TT_Cluster_Populations as TTCP
                INNER JOIN #TT_Cluster_Descriptions as TTCD
                ON TTCP.Cluster_ID = TTCD.Cluster_ID
                ORDER BY 1,2

            -- ========================================
            -- Cluster/Refset Content
            -- ========================================

            -- Returns all codes active in the PCD refsets for all those clusters
            (SELECT DISTINCT nq2.[Cluster_ID], rfo.[refsetId] as [PCD Refset ID], rfo.[referencedComponentId] as [SNOMED code], sd.term as [SNOMED code description]
            INTO #TT_PCD_Refset_Content
            FROM
                (SELECT [id], MAX([effectiveTime]) as [MAXeffectiveTime]  
                FROM {LocalSNOMEDCT.Refsets}
                WHERE effectiveTime <> '99999999'										   	
                GROUP BY [id]
                ) as nq11											-- This subquery finds the most recent combination of id, refset ID and SNOMED code from the Full Refsets table
            INNER JOIN {LocalSNOMEDCT.Refsets} as rfo
                ON nq11.MAXeffectiveTime = rfo.effectiveTime
                AND nq11.id = rfo.id
            LEFT JOIN {UKSNOMEDCT.SCT_DESCRIPTION} as sd 
                ON rfo.referencedComponentId = sd.conceptId
                AND sd.DescriptionType = 'F'					-- Returns only the fully specified name (F) for the SNOMED code description.
                AND sd.[active] = 1							-- Returns only the active (1) SNOMED code descriptions.
            INNER JOIN #TT_Cluster_Descriptions nq2
                ON rfo.refsetId = nq2.Refset_ID
            WHERE rfo.active = 1)						-- Returns only active (1) refset content.

            UNION -- Joins above table to the results of the one below
            -- Query to return the Cluster_ID and Refset_ID of all drug clusters.  SNOMED code and SNOMED code description columns added to indicate these clusters are drug ones and have no content showing as the PCD
            -- do not maintain these ones.

            (SELECT DISTINCT clu.[Cluster_ID], clu.[Refset_ID], 
            'Check the SNOMED browser for refset content' as [SNOMED code],
            'Refset not managed or maintained by the PCD team'as [SNOMED code description]
            FROM {ClusterManagement.Clusters} as clu
            INNER JOIN {ClusterManagement.Cluster_Ruleset} as cru
                ON clu.Cluster_ID = cru.Cluster_ID
            INNER JOIN {ClusterManagement.Ruleset_Published} as p
                ON cru.Ruleset_ID = p.Ruleset_ID COLLATE Latin1_General_CI_AS 
                AND cru.Ruleset_Version = p.Ruleset_Version
                AND cru.Service_ID = p.Service_ID COLLATE Latin1_General_CI_AS
            WHERE clu.[Cluster_Date_Inactive] IS NULL
                AND clu.Refset_ID NOT LIKE '%100023010%'
                AND ((p.Ruleset_Publication_Inactive IS NULL OR p.Ruleset_Publication_Inactive > (SELECT getdate())))
                AND p.[Ruleset_Publication_Effective] <= (select getdate()
            ))      

            ORDER BY 1

            -- ============================================
            -- Final Table
            -- ============================================

            SELECT TTOPC.*, TTPRC.[SNOMED code] as [SNOMED_code], TTPRC.[SNOMED code description] as [SNOMED_code_description], TTPRC.[PCD Refset ID]
            FROM #TT_Output_Pop_Clusters as TTOPC
            INNER JOIN #TT_PCD_Refset_Content as TTPRC
            ON TTOPC.Cluster_ID = TTPRC.Cluster_ID
            ORDER BY 1
            """
    return query
