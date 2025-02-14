from pathlib import PurePath
import shutil
import pandas as pd
import openpyxl
import logging

from src.utils.file_fxns import excel_save
from src.utils.connection_fxns import get_df_from_sql

def pbi_addsheet(destfile,df,db,pbifldr,cg,rstype):

    """
    Code to update files
    Takes a path (location of file) and filename of a workbook, a dataframe, a database, a folder location for Power BI tables, the location of local Confluence guidance, and a ruleset/service type.
    Adds a new sheet to the source workbook (does not over-write) so that the old/new sheets can be compared,
    and any relevant amendments made. 
    Returns a string with the name and location of the new sheet
    """
    
    with pd.ExcelWriter(destfile, mode = 'a', engine = 'openpyxl', if_sheet_exists='new') as writer: 
        book = openpyxl.load_workbook(destfile) #load the existing workbook to make the amendments
        writer._book = book
        db_tab_name = f"{db}"
        df.to_excel(writer, sheet_name=db_tab_name, index = False)

        #update and determine worksheet criteria
        infosheetname = book.sheetnames[-1] #the NAME of the latest sheet added, in case previous infosheets haven't been deleted (new ones have the same name plus a number if old ones haven't been deleted).
        infosheet = book[infosheetname]  # the latest WORKSHEET appended, where instructions will be added
        infosheet.insert_rows(idx=1, amount=29)
        infosheet["A1"].value = f"Instructions for {rstype}_Full_Name_Mappings table amendments:"
        infosheet['A1'].font = openpyxl.styles.Font(bold=True, size=14)
        infosheet["A2"].value = f"The list below shows the {rstype.lower()} content of the Ruleset_published table in the SQL database."
        infosheet["A3"].value = f"Compare this list with the existing content on the {rstype}_Full_Names worksheet. Updates may be needed to the SQL table or the {rstype}_Full_Names table in {pbifldr}."
        infosheet["A5"].value = f"If there is a {rstype.lower()} in the {rstype}_Full_Names tab but not in the {db_tab_name} tab:"
        infosheet['A5'].font = openpyxl.styles.Font(bold=True)
        infosheet["A6"].value = f"Possible reasons:"
        infosheet["A8"].value = f"1. (Most likely) This is a retired {rstype.lower()}."
        infosheet['A8'].font = openpyxl.styles.Font(bold=True, color = '005EB8')
        infosheet["A9"].value = f"Check this in the {db} Rulesets table and if it is a retired {rstype.lower()} then remove from the {rstype}_Full_Names worksheet in "
        infosheet["A10"].value = f"{pbifldr}/{rstype}_Full_Name_Mappings.xlsx"
        infosheet["A12"].value = f"2. This is a new {rstype.lower()} which hasn't been updated in the {db} Ruleset_published table."
        infosheet['A12'].font = openpyxl.styles.Font(bold = True, color = '005EB8')
        infosheet["A13"].value = f"Check this in the {db} Rulesets table and if it is a new {rstype.lower()} then update the {db} Ruleset_published table."
        infosheet["A14"].value = f"Guidance to update Ruleset_published table can be found in {cg}"
        infosheet["A16"].value = f"If there is a {rstype.lower()} in the {db_tab_name} tab but not in the {rstype}_Full_Names tab:"
        infosheet['A16'].font = openpyxl.styles.Font(bold=True)
        infosheet['A17'].value = f"Possible reasons:"
        infosheet["A19"].value = f"1. (Most likely) This is a new {rstype.lower()}."
        infosheet['A19'].font = openpyxl.styles.Font(bold=True, color = '005EB8')
        infosheet["A20"].value = f"Check this in the {db} Rulesets table and if it is a new {rstype.lower()} then add to the {rstype}_Full_Names worksheet in "
        infosheet["A21"].value = f"{pbifldr}/{rstype}_Full_Name_Mappings.xlsx"
        if rstype.lower() == 'ruleset':
            infosheet["A22"].value = f"When adding a new entry to the worksheet, if the {rstype.lower()} has an abbreviation then look to add the {rstype}_Full_Name as the full name followed by the abbreviation in brackets e.g. Coronary Heart Disease (CHD)."
            infosheet["A23"].value = f"Add the new entry where it sits alphabetically in the list."
        else:
            infosheet["A22"].value = f"When adding a new entry to the worksheet, if the {rstype.lower()} has an abbreviation then look to add the {rstype}_Full_Name as the full name followed by the abbreviation in brackets e.g. Core Contract (CC)"
            infosheet["A23"].value = f"Amend the display order if necessary (the most 'popular' services are displayed first). If unsure on where to place the new service in this ordering, raise within the team."
        infosheet["A25"].value = f"2. This is a retired {rstype.lower()} which hasn't been removed from the {db} Ruleset_published table."
        infosheet['A25'].font = openpyxl.styles.Font(bold=True, color = '005EB8')
        infosheet["A26"].value = f"Check this in the {db} Rulesets table and if it is a retired {rstype.lower()} then update the {db} Ruleset_published table."
        infosheet["A27"].value = f"Guidance to update Ruleset_published table can be found in {cg}"
        infosheet["A29"].value = f"List of {rstype.lower()}s from the Ruleset_published table:"
        infosheet['A29'].font = openpyxl.styles.Font(bold=True)
      
        for cells in infosheet.rows:
            for cell in cells:
                cell.border = None
                cell.alignment = openpyxl.styles.Alignment(horizontal='left')
        infosheet.sheet_properties.tabColor = 'FFFF00'
        infosheet.sheet_view.showGridLines = False
        infosheet.column_dimensions['A'].width = 45
        book.active = infosheet

    alertText = f"ACTION REQUIRED - new worksheet added to {destfile}.\nPlease check the '{infosheetname}' worksheet for instructions, and update as necessary."
      
    return alertText


#-------------------------------------------------------------------------------------------------------------------
def sql_to_excel_pbi(sql_query, database, save_filepath, save_filename):

    PBI_df = get_df_from_sql(server=database.server, 
                             database=database.db, 
                             query=sql_query(database))

    #Export to Excel - Shared drive
    excel_save(df=PBI_df, 
            filepath= save_filepath,
            filename= save_filename)
    
    logging.info(f'Power BI {save_filename[:-8]} export complete.') 
    
    return PBI_df     
   
#-------------------------------------------------------------------------------------------------------------------
def pbi_change_check(compare_df, rstype, rstype_df, filepath_dict, NameMapSourceLoc, cmdb, config):
    """
    Inputs:
        - Takes a comparison dataframe
        - a 'Service'/'Ruleset' variable
        - a Service/Ruleset IDs dataframe
        - a filepath dictionary
        - a source filepath of a Full_Name_Mappings.xlsx
        - the Cluster Management database
        - config dictionary.
    Actions:
        - Checks each cell in the comparison dataframe; if the cell contains "False", appends the index of the applicable row to a list.
        - Assigns True/False to a ChangeFlag variable
        - If applicable, copies the Full_Name_Mappings.xlsx file to cwd and adds a tab with instructions for investigating and amending the Full_Name_Mappings table in the shared drive.
        - Creates an AlertText string with instructions for any amendments required.
    Outputs:
        Returns the ChangeFlag variable and the AlertText string.
    """
    index_list = []
    ChangeFlag = False
    for index,row in compare_df.iterrows():
        for UID in row:
            if UID == False: 
                index_list.append(index) 
    if len(index_list) != 0:
        ChangeFlag = True
        # If differences are found, copy the relevant PBI_Full_Name_Mappings.xlsx to cwd, so that error tab can be added. 
        # Will only be moved back to the Shared drive later in the job where test mode = false
        NameMapDestLoc = PurePath(filepath_dict['output_fldr'], f"PBI_{rstype}_Full_Name_Mappings.xlsx")
        shutil.copyfile(NameMapSourceLoc,NameMapDestLoc)
        AlertText = f"PBI_{rstype}_Full_Name_Mappings table:\n" + pbi_addsheet(NameMapDestLoc,rstype_df,str(cmdb),config['Filepaths']['PowerBI_xlsx_Outputs'],config['PowerBI']['Confluence_guidance'],rstype)
    else:
        logging.info(f"Currently active {rstype.lower()} types match the Power BI Full Name Mapping tables - no changes necessary")
        AlertText = ""
    return ChangeFlag, AlertText
#-------------------------------------------------------------------------------------------------------------------

def pbi_refset_reldate_table(config, filepath_dict):
    """
    Takes the config file and filepath dictionary
    Creates a dataframe containing the current PCD release date for use as a card in the Power BI report
    Exports the dataframe as an excel spreadsheet to a shared drive
    """
    
    RefsetRelDate_data = {
    "This content is based on the following PCD TRUD release:": [config['Dates']['PCDreleaseDate']]
    }
    RefsetRelDate_df = pd.DataFrame(RefsetRelDate_data)
    excel_save(RefsetRelDate_df, filepath= filepath_dict['PowerBIxlsx_fldr'], filename= 'PBI_TRUD_Release_Date_df.xlsx')
    logging.info(f'Creation and export of PBI_TRUD_Release_Date_df.xlsx table complete.')
    return RefsetRelDate_df  