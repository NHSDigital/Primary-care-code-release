#import docx
import logging
#from docx.enum.dml import MSO_THEME_COLOR_INDEX
from pathlib import PurePath
from src.utils.dates import class_Dates
from src.utils.databases import class_LocalSNOMEDCT_dbs
from sql.sql_simple_queries import sql_query_add_remov_refset, sql_query_TRUD_total_refsets
from src.utils.connection_fxns import get_df_from_sql
from src.utils.file_fxns import create_word_doc, save_word_doc,add_hyperlink

#Stage 8: Supporting Information Word Document
def supportingInfo(configDates:class_Dates, filepath_dict:dict, rel_version:str, config_documents:dict, LocalSNOMEDCT:class_LocalSNOMEDCT_dbs):
    """
    Function Actions:
    - Produces the UK NHS Primary Care data extraction reference sets supporting information document.
    """
    logging.info('Supporting Information document being produced...')

    #define dates from dates dictionary    
    Documentation_fldr = filepath_dict['Documentation_fldr']
    std_temp_fldr = filepath_dict['std_temp_fldr']

    # How many added or removed refsets within latest release
    add_remov_refset_data = get_df_from_sql(server=LocalSNOMEDCT.server,
                                            database=LocalSNOMEDCT.db,
                                            query=sql_query_add_remov_refset(LocalSNOMEDCT))

    #df created to be used to update the wording for the word file.
    refsets_numbers = add_or_remove_refsets(add_remov_refset_data)

    #---------------------------------------------------------------------------------------------
    # Current active refsets
    active_refsets_tbl = f"{LocalSNOMEDCT.db}.dbo.sct2_Concept_Snapshot_1000230_{configDates.YYYYMMDD(configDates.PCDreleaseDate)}"
    Total_TRUD_refset_data = get_df_from_sql(server=LocalSNOMEDCT.server,
                                             database=LocalSNOMEDCT.db,
                                             query=sql_query_TRUD_total_refsets(active_refsets_tbl))

    #assign total number of active refset to vairable
    Total_TRUD_refset_data = Total_TRUD_refset_data.iloc[0]['active_refsets']
    refsets_numbers["total refsets"] = Total_TRUD_refset_data
    
    logging.info("Database & Table: " + active_refsets_tbl)
    logging.info("Total refsets: " + str(Total_TRUD_refset_data))

    #---------------------------------------------------------------------------------------------
    # read in word template
    template = PurePath(std_temp_fldr, "TEMPLATE_UKSNOMEDCTPrimaryCareDataExtractionsOverviewNHSE.docx")
    
    #dictionary of the content to be updated to in the word document
    content = {'rel_version':f"Release version {rel_version}",
               'rel_dat':f"Published {configDates.ddMonthYYYY(configDates.PCDreleaseDate)}"}
    #create Supporting Information document, based off template
    word_document = create_word_doc(template=template, YYYY=configDates.YYYY(configDates.PCDreleaseDate), content=content, par=1, business_name=config_documents['business_name']) #generic function
    #edit the newly created Supporting Information document word document with above content
    word_document = edit_UK_snmed_wrd_doc(document=word_document, configDates=configDates, refsets_numbers=refsets_numbers, config_documents=config_documents) #specific function
    save_word_doc(document=word_document, outfile_path=Documentation_fldr, outfile_name=F"doc_UKSNOMEDCTPrimaryCareDataExtractionsOverview_Current-en_GB_{configDates.YYYYMMDD(configDates.PCDreleaseDate)}.docx") #generic function
    
    logging.info('Supporting Information document stage complete.')
    
#------------------------------------------------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------------------------------------------------

def add_or_remove_refsets(add_remov_refset_data):
    """
    Function Actions:
    - Returns dataframe with wording to add in to word document based off singular/plural refsets added/removed.
    """
    logging.info(add_remov_refset_data.head)
    if add_remov_refset_data.shape[0] ==0:
        added_refsets = 0
    elif len(add_remov_refset_data.loc[add_remov_refset_data["active status"] == "1"]) != 0:
        added_refsets = add_remov_refset_data.loc[add_remov_refset_data["active status"] == "1"].iloc[0]["refsets"]
    else:
        added_refsets = 0

    if add_remov_refset_data.shape[0] ==0:
        removed_refsets = 0    
    elif len(add_remov_refset_data.loc[add_remov_refset_data["active status"] == "0"]) != 0:
        removed_refsets = add_remov_refset_data.loc[add_remov_refset_data["active status"] == "0"].iloc[0]["refsets"]
    else:
        removed_refsets = 0
        
    if added_refsets == 1:
        sin_plu_add = 'refset has'
    else:
        sin_plu_add = 'refsets have'
    if removed_refsets == 1:
        sin_plu_rem = 'refset has'
    else:
        sin_plu_rem = 'refsets have'
    
    #new df with updates for supporting information doc 
    refsets_numbers = {
        'added refsets':added_refsets, 
        'removed refsets':removed_refsets,
        'singular or plural add':sin_plu_add, 
        'singular or plural remove':sin_plu_rem
        }
        
    logging.info("Added refsets since last release: " + str(added_refsets))
    logging.info("Removed refsets since last release: " + str(removed_refsets))
    
    return refsets_numbers

#------------------------------------------------------------------------------------------------------------------------------------------
#add in document type in function parameters if possible
def edit_UK_snmed_wrd_doc(document, configDates:class_Dates, refsets_numbers, config_documents):
    """
    Function Actions:
    - Adds standard information to the end of the supporting information document.
    - Adds in information about the number of refsets added/removed from this release.
    - Adds in any additional information, as defined in the config file.
    """
    #gather vairables from config file for this function
    add_info_para = config_documents['additional_info']
    add_team_phone_tel = config_documents['team_phone_tel']
    add_team_address = config_documents['team_address']
    add_team_email = config_documents['team_email']

    #add content and links to word document...
    document.add_paragraph("These UK NHS Primary Care data extraction reference sets are based on the concepts included in " + configDates.ddMonthYYYY(configDates.UKreleaseDate) + " UK Clinical Edition of the UK Edition of SNOMED CT®. This document relates to Release Format 2 (RF2).\n")

    p = document.add_paragraph()
    p.add_run("For a full description of the technical structure, use and implementation of SNOMED CT; please refer to the detailed documentation that accompanies the release, the technical reference guides on the ")
    add_hyperlink(p, "UK SNOMED CT website", "https://digital.nhs.uk/services/terminology-and-classifications/snomed-ct")
    p.add_run(" and the ")
    add_hyperlink(p, "SNOMED International guidance", "https://www.snomed.org/snomed-international/learn-more")
    p.add_run(", in particular the Technical Implementation Guide. Additional technical documentation is published on ")
    add_hyperlink(p, "Delen", "https://hscic.kahootz.com/connect.ti/t_c_home/browseFolder?fid=16607376")
    p.add_run(".\n")

    q = document.add_paragraph()
    q.add_run("These refsets are primarily used to support various primary care extractions and frameworks as detailed in NHS England ‘business rules’.  More details on the business rules can be found on the NHS England ")
    add_hyperlink(q, "website", "https://digital.nhs.uk/data-and-information/data-collections-and-data-sets/data-collections/quality-and-outcomes-framework-qof")
    q.add_run(". The Supporting Products folder contains additional files which are used by NHS England in the management and analysis of primary care extracts.\n")


    document.add_paragraph(f"In this {configDates.ddMonthYYYY(configDates.PCDreleaseDate)} UK NHS Primary Care data extraction release, " 
                           + str(refsets_numbers["added refsets"]) 
                           + " new " 
                           + refsets_numbers["singular or plural add"] 
                           + " been added and " + str(refsets_numbers["removed refsets"]) 
                           + " " + refsets_numbers["singular or plural remove"] 
                           + " been made inactive. There are now a total of " 
                           + str(refsets_numbers["total refsets"]) 
                           + " active refsets in the UK NHS Primary Care data extraction release.\n")
    document.add_paragraph(f"The delta for this {configDates.ddMonthYYYY(configDates.PCDreleaseDate)} UK NHS Primary Care data extraction release is based on changes since the " + configDates.ddMonthYYYY(configDates.PrevPCDrelDate) + " UK NHS Primary Care data extraction release.\n")

    document.add_paragraph("Additional notes: " + add_info_para)
    document.add_paragraph("\n")
    document.add_paragraph("For any enquiries about the UK NHS Primary Care data extraction release, please contact the GPDESA team via the following:")
    document.add_paragraph(f"Tel: {add_team_phone_tel}")
    r = document.add_paragraph()
    r.add_run("Email: ")
    add_hyperlink(r, f"{add_team_email}", f"mailto:{add_team_email}")
    document.add_paragraph(f"Address: {add_team_address}")

    return document


