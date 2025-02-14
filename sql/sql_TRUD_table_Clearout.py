from src.utils.databases import class_LocalSNOMEDCT_dbs

#13-------------------------------------------------------------------------------------------------------------------
#TRUD_PostPub_Cleardown.py
def sql_query_TRUD_Table_Clearout(LocalSNOMEDCT:class_LocalSNOMEDCT_dbs, Oldest_date):
    
    query = f"""
        SET NOCOUNT ON; SET ANSI_WARNINGS OFF
        
        Use {LocalSNOMEDCT.db}
        
        --Delete Old Release Tables

        Drop Table if exists [dbo].[der2_cciRefset_RefsetDescriptorDelta_1000230_{Oldest_date}]
        Drop Table if exists [dbo].[der2_cciRefset_RefsetDescriptorFull_1000230_{Oldest_date}]
        Drop Table if exists [dbo].[der2_cciRefset_RefsetDescriptorSnapshot_1000230_{Oldest_date}]
        Drop Table if exists [dbo].[der2_cRefset_LanguageDelta-en_1000230_{Oldest_date}]
        Drop Table if exists [dbo].[der2_cRefset_LanguageFull-en_1000230_{Oldest_date}]
        Drop Table if exists [dbo].[der2_cRefset_LanguageSnapshot-en_1000230_{Oldest_date}]
        Drop Table if exists [dbo].[der2_Refset_SimpleDelta_1000230_{Oldest_date}]
        Drop Table if exists [dbo].[der2_Refset_SimpleFull_1000230_{Oldest_date}]
        Drop Table if exists [dbo].[der2_Refset_SimpleSnapshot_1000230_{Oldest_date}]
        Drop Table if exists [dbo].[der2_ssRefset_ModuleDependencyDelta_1000230_{Oldest_date}]
        Drop Table if exists [dbo].[der2_ssRefset_ModuleDependencyFull_1000230_{Oldest_date}]
        Drop Table if exists [dbo].[der2_ssRefset_ModuleDependencySnapshot_1000230_{Oldest_date}]
        Drop Table if exists [dbo].[sct2_Concept_Delta_1000230_{Oldest_date}]
        Drop Table if exists [dbo].[sct2_Concept_Full_1000230_{Oldest_date}]
        Drop Table if exists [dbo].[sct2_Concept_Snapshot_1000230_{Oldest_date}]
        Drop Table if exists [dbo].[sct2_Description_Delta-en_1000230_{Oldest_date}]
        Drop Table if exists [dbo].[sct2_Description_Full-en_1000230_{Oldest_date}]
        Drop Table if exists [dbo].[sct2_Description_Snapshot-en_1000230_{Oldest_date}]
        Drop Table if exists [dbo].[sct2_Relationship_Delta_1000230_{Oldest_date}]
        Drop Table if exists [dbo].[sct2_Relationship_Full_1000230_{Oldest_date}]
        Drop Table if exists [dbo].[sct2_Relationship_Snapshot_1000230_{Oldest_date}]
        Drop Table if exists [dbo].[sct2_StatedRelationship_Delta_1000230_{Oldest_date}]
        Drop Table if exists [dbo].[sct2_StatedRelationship_Full_1000230_{Oldest_date}]
        Drop Table if exists [dbo].[sct2_StatedRelationship_Snapshot_1000230_{Oldest_date}]
        
                    """
    return query
#-------------------------------------------------------------------------------------------------------------------

