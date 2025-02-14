#stage 10 process
import logging
import os
#import docx
from email.mime.text import MIMEText
from pathlib import PurePath
from datetime import datetime, timedelta
from src.utils.file_fxns import create_word_doc, save_word_doc, email 
from src.utils.dates import class_Dates

def TRUD_pack_req(configDates:class_Dates, filepath_dict:dict, rel_version:str, email_dict:dict, document_info:dict, Name:str):
    """
    Function Actions:
    - Creates a change request form (New TRUD Pack Request) word document.
    - Generates an email, with the zipped file and change request form, ready to be sent to the Data Architects. 
    """
    logging.info("TRUD pack request document being produced...")

    #define filepaths from dictionary
    output_fldr = filepath_dict['output_fldr']
    std_temp_fldr = filepath_dict['std_temp_fldr']
    publishing_TRUD_fldr = filepath_dict['publishing_TRUD_fldr']
    uk_sct2pc_ext = filepath_dict['uk_sct2pc_ext']

    #define inputs needed to create and save word doc
    TRUD_Doc_name = "TRUD release publication request.item=659.release-date=.docx"
    Target_TRUD_Date = datetime.today() + timedelta(days = 10) #minimum of 2 weeks notice required by TRUD - update days = if this changes
    Target_TRUD_Date = Target_TRUD_Date.strftime('%Y%m%d')

    #update string variables for the email content
    content = {"Target_TRUD_Date": configDates.ddMonthYYYY(Target_TRUD_Date), 
                 "xx.x.x": rel_version,
                 "UK_rel_dat": configDates.ddMonthYYYY(configDates.UKreleaseDate),
                 "<<Jira_contact>>": document_info['reciever_email'],
                 "<<Team_owner_contact>>": document_info['team_email']}
    
    #create and save TRUD request pack word doc
    document = create_word_doc(template=PurePath(std_temp_fldr, TRUD_Doc_name), YYYY=configDates.YYYY(configDates.PCDreleaseDate), content=content, par=0, business_name=document_info['business_name'])
    TRUD_Doc_name = TRUD_Doc_name[:-5] + configDates.YmdDashes(Target_TRUD_Date) + TRUD_Doc_name[-5:]    
    save_word_doc(document=document, outfile_path=publishing_TRUD_fldr, outfile_name=TRUD_Doc_name)

    #Email body
    body = email_dict['body']
    body = body.replace("<Target_TRUD_Date>", configDates.ddMonthYYYY(Target_TRUD_Date))
    body = MIMEText(body, 'plain', 'utf-8')
    
    attachment_list = [PurePath(output_fldr, uk_sct2pc_ext + '.zip'), PurePath(publishing_TRUD_fldr, TRUD_Doc_name)]
    
    #final composition of email
    email(email_subject=email_dict['subject'], 
          email_To=email_dict['To'], 
          email_Cc=email_dict['Cc'], 
          email_body=body, 
          outfile_path=publishing_TRUD_fldr, 
          outfile_name=f"Zipped Refset Release v{rel_version}.eml", 
          attach=True, 
          attachment_files=attachment_list)   