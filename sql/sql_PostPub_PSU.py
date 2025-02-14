from src.utils.dates import class_Dates
from src.utils.databases import class_UKSNOMEDCT_dbs, class_LocalSNOMEDCT_dbs

#9-------------------------------------------------------------------------------------------------------------------
#update_SCT.py
def sql_query_PostPub_PSU_db_updates(UKSNOMEDCT:class_UKSNOMEDCT_dbs, LocalSNOMEDCT:class_LocalSNOMEDCT_dbs, configDates:class_Dates):
    """
    Function Actions:
    - Updates SCT_CONCEPT, SCT_REFSET, SCT_DESCRIPTION tables where moduleId = '999000011000230102' with the new release's snapshot table's data.
    """

    query = f"""
    
        SET NOCOUNT ON; SET ANSI_WARNINGS OFF --required for reading code through python

        --Concept
        DELETE FROM {UKSNOMEDCT.SCT_CONCEPT}
        WHERE moduleId = '999000011000230102'

        INSERT INTO {UKSNOMEDCT.SCT_CONCEPT}
        SELECT 'PCD_Refsets' as [SourceData], * 
        FROM {LocalSNOMEDCT.db}.dbo.sct2_Concept_Snapshot_1000230_{configDates.YYYYMMDD(configDates.PCDreleaseDate)}

        --Description
        DELETE FROM {UKSNOMEDCT.SCT_DESCRIPTION}
        WHERE moduleId = '999000011000230102'

        INSERT INTO {UKSNOMEDCT.SCT_DESCRIPTION}
        SELECT *, CASE WHEN typeId = '900000000000003001' THEN 'F' ELSE 'P' END as [DescriptionType]  
        FROM {LocalSNOMEDCT.db}.dbo.[sct2_Description_Snapshot-en_1000230_{configDates.YYYYMMDD(configDates.PCDreleaseDate)}]

        --Refset
        DELETE FROM {UKSNOMEDCT.SCT_REFSET}
        WHERE moduleId = '999000011000230102'

        INSERT INTO {UKSNOMEDCT.SCT_REFSET}
        SELECT 'PCD_Refsets' as [SourceData], * 
        FROM {LocalSNOMEDCT.db}.dbo.der2_Refset_SimpleSnapshot_1000230_{configDates.YYYYMMDD(configDates.PCDreleaseDate)}

            """
    return query
#-------------------------------------------------------------------------------------------------------------------
