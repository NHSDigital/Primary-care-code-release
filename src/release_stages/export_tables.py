import os
import pandas as pd
import logging
from pathlib import PurePath

from src.utils.dates import class_Dates
from src.utils.databases import class_LocalSNOMEDCT_dbs
from src.utils.connection_fxns import get_df_from_sql
from sql.sql_simple_queries import sql_query_select_distinct

def export_tables(configDates:class_Dates, filepath_dict:dict, SnomedCT_Folders_Creation_vars, LocalSNOMEDCT:class_LocalSNOMEDCT_dbs):
    """
    Function Actions:
    - Exports the new release SQL tables.
    - Saves these as txt files to a folder structure beginning with "SnomedCT_UKPrimaryCareRF2_PRODUCTION_YYYYMMDDT000000Z", in the TRUD_PCD_SNOMED Release Files folder on the shared Drive.
    - Checks the correct number of files and folders have been created.
    """
    logging.info('Creating SNOMEDCT folder structure and exporting SQL tables...')

    csv = SnomedCT_Folders_Creation_vars['csv_name']
    folder_count = SnomedCT_Folders_Creation_vars['folder_count']

    #read in csv from the templates folder
    data = pd.read_csv(PurePath(filepath_dict['std_temp_fldr'], csv))
    
    #count number of files for use in error checking later
    data_row_count = len(data)
    
    #create folders structure to save txt files into
    path = create_folders(dir=filepath_dict['output_fldr'],main_folder=filepath_dict['SNOMEDCT_UK_ext'])
    
    #loop through populated tables, pull from SQL server and write to SNOMEDCT folder
    query_tables = data.loc[data['Content'] == 'Populated']
    for index, row in query_tables.iterrows():
        Table_name = query_tables.at[index, 'SQL_Name'] + configDates.YYYYMMDD(configDates.PCDreleaseDate)
        Export_name = query_tables.at[index, 'Export_Name'] + configDates.YYYYMMDD(configDates.PCDreleaseDate) + '.txt'
        Export_path = PurePath(path,query_tables.at[index, 'Path'])
        table_to_export = get_df_from_sql(server=LocalSNOMEDCT.server,
                                          database=LocalSNOMEDCT.db,
                                          query=sql_query_select_distinct(database=LocalSNOMEDCT.db, #create_pcd_tables stage creates a copy of all of these in the test area
                                                                          table_name=Table_name))
        try: write_file(table_to_export, Export_name, Export_path)
        except FileExistsError:
            #if files already exist, process will stop, to avoid accidental re-running/overwriting
            logging.info("SNOMEDCT folder: Populated tables creation stopped - Files already exist")
            break

    #loop through empty tables, create txt files with column names
    empty_tables = data.loc[data['Content'] == 'Empty'].reset_index()  
    for index, row in empty_tables.iterrows():                  
        row_len = empty_tables.iloc[int(str(index)),:].count()
        values = range(6,row_len)                             
        dfcols = [empty_tables.iat[index,5]]   
        for i in values:
            next_col = [empty_tables.iat[index,i]]
            dfcols = dfcols + next_col
        df = pd.DataFrame(columns = [dfcols])
        Export_name = empty_tables.at[index, 'Export_Name'] + configDates.YYYYMMDD(configDates.PCDreleaseDate) + '.txt'
        Export_path = PurePath(path,empty_tables.at[index, 'Path'])
        #write and save down each table as an individual text file 
        try: write_file(df,Export_name,Export_path)
        except FileExistsError:
            #if files already exist, process will stop, to avoid accidental re-running/overwriting
            logging.info("SNOMEDCT folder: Empty tables creation stopped - Files already exist")
            break
    
    #error checking that number of files / folders is as expected
    final_checks(path, data_row_count, folder_count)

    logging.info('Exporting Code Release Tables Stage complete.')

#------------------------------------------------------------------------------------------------

def create_folders(dir, main_folder):
    """
    Function Actions:
    - Confirms currently directory folderpath.
    - Creates folders based off structure given from filepaths_dict within the Repository outputs folder.
    """
    #define current directory so can change back at the end of the function and change directory to root folder
    current = os.getcwd()
    os.chdir(dir)
    
    #folder structure
    #main folder taken from filepaths_dict
    main_dir = ["Delta", "Full", "Snapshot"]
    common_dir = "Refset"
    common_dir2 = ["Content", "Language", "Map", "Metadata"]
    common_dir3 = "Terminology"
    common_dir4 = ["Documentation","SupportingProducts"]

    #loop through folders to create folder structure
    for dir3 in main_dir:
        for dir5 in common_dir2:
            try: os.makedirs(os.path.join(main_folder,dir3,common_dir,dir5))
            except OSError: pass

    for dir3 in main_dir:
        try: os.makedirs(os.path.join(main_folder,dir3,common_dir3))
        except OSError: pass

    for dir3 in common_dir4:
        try: os.makedirs(os.path.join(main_folder,dir3))
        except OSError: pass

    path = PurePath(dir,main_folder) #this is the same as filepath_dict['SNOMEDCT_UK_fldr']
    os.chdir(current) #change directory back to previous
    logging.info('SNOMEDCT folder structure created successfully.')

    return path

#------------------------------------------------------------------------------------------------

def write_file(Table_to_export:pd.DataFrame, Export_name, Export_path):
    """
    Function Actions:
    - Writes a dataframe to a filepath.
    - Separated by tabs
    - Throws up an error if file already exists so as not to overwrite.
    """
    #write table as .txt file to Shared drive - note will error and stop if filename already exists
    Table_to_export.to_csv(PurePath(Export_path, Export_name), index = False, sep="\t", mode='x')

#------------------------------------------------------------------------------------------------

def final_checks(path, data_row_count, folder_count):
    """
    Function Actions:
    - Finds the number of folders and files in a directory.
    - If folder number and file number are as expected, prints 'Pass', otherwise 'Fail'.
    """
    #check we have correct number of files and folders
    count_files = 0                      
    count_dirs = 0
    #loops through each level of folder and keeps adding how many files and folders it finds
    for root_dir, cur_dir, files in os.walk(path): 
        count_files += len(files)
        count_dirs += len(cur_dir)
    
    if count_files == data_row_count and count_dirs == folder_count: #as specified to match in the Config file
        output2 = "Pass"
    else: output2 = "Error"
    
    #changed to f-string
    output = f"""Export Tables; Stage 5:
    Final checks returned:
    Number of folders: {str(count_dirs)}
    Number of files: {str(count_files)}
    Outcome: {output2}."""
        
    logging.info(output)   