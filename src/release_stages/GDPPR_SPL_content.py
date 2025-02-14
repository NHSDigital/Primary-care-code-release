import logging
from pathlib import PurePath
from src.utils.dates import class_Dates
from src.utils.databases import class_UKSNOMEDCT_dbs, class_LocalSNOMEDCT_dbs, class_ClusterManagement_dbs
from sql.sql_GDPPR_C19SPL_changes import sql_query_GDPPR_C19SPL_changes
from src.utils.connection_fxns import get_df_from_sql
from src.utils.file_fxns import excel_save, email
from email.mime.text import MIMEText

def GDPPR_SPL_content(configDates:class_Dates, publishing_TRUD_fldr:str, rel_version, configEmails:dict, UKSNOMEDCT:class_UKSNOMEDCT_dbs, LocalSNOMEDCT:class_LocalSNOMEDCT_dbs, ClusterManagement:class_ClusterManagement_dbs):
    """
    Function Actions:
    - C19 SNOMED (GDPPR/SPL cluster content) changes file is produced.
    - File is attached in an email generated.
    - An email is written to GPDESA about the suppliers Sharepoint site. The suppliers Sharepoint updates will need to be done manually after this script.
    """
    #Stage 12: GDPPR / SPL cluster content changes
    logging.info('GDPPR SQPL content updates incoming...')

    # run sql query from file
    C19SPL_chnges_data = get_df_from_sql(server=LocalSNOMEDCT.server,
                                         database=LocalSNOMEDCT.db,
                                         query=sql_query_GDPPR_C19SPL_changes(UKSNOMEDCT, 
                                                                                 LocalSNOMEDCT, 
                                                                                 ClusterManagement))

    filename_c19_changes = f"C19_SNOMED_changes_{configDates.YYYYMMDD(configDates.PCDreleaseDate)}.xlsx"
    ##creates emails and saves them down
    excel_save(df=C19SPL_chnges_data, filename=filename_c19_changes, filepath=publishing_TRUD_fldr)

    #--------------------------------------------------
    #Email_3: Email set up for: GDPPR SPL cluster content changes 
    email_dict3 = configEmails['Email_3']
    
    attachment_list = [PurePath(publishing_TRUD_fldr, filename_c19_changes)]
    
    #composition of email 3
    email(email_subject=f"{email_dict3['subject']} v{rel_version} ({configDates.YYYYMMDD(configDates.PCDreleaseDate)})", 
          email_To=email_dict3['To'], 
          email_Cc=email_dict3['Cc'], 
          email_body=MIMEText(email_dict3['body'],'plain', 'utf-8'), 
          outfile_path=publishing_TRUD_fldr, 
          outfile_name=f"GDPPR SPL cluster content changes {configDates.YYYYMMDD(configDates.PCDreleaseDate)}.eml", 
          attach=True, 
          attachment_files=attachment_list)
    #--------------------------------------------------
    #Email_4: Email set up for: Suppliers Sharepoint
    email_dict4 = configEmails['Email_4']

    #Email body
    body = email_dict4['body']
    body = body.replace('<rel_version>', f'v{rel_version}')
    body = MIMEText(body, 'plain', 'utf-8')

    #composition of email 4
    email(email_subject=email_dict4['subject'], 
          email_To=email_dict4['To'], 
          email_Cc=email_dict4['Cc'], 
          email_body=body, 
          outfile_path=publishing_TRUD_fldr, 
          outfile_name=f"{email_dict4['subject']} v{rel_version}.eml", 
          attach=False, 
          attachment_files=[])
    #--------------------------------------------------
    logging.info('GDPPR SQPL content email updates completed.')