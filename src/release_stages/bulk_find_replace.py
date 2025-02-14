# import os
# import logging
# from pathlib import PurePath
# from src.utils.dates import class_Dates

# def bulk_find_replace(configDates:class_Dates, sql_pub_fldr):

#     """
#     Function Actions:
#     - Copies template document and template sql queries into publication folder. This is test_folder in test mode.
#     - Remove the 'Archive' folder in the Publication folder that was carried over from the Templates folder.
#     - Performs standard bulk find and replace: all dates across all sql queries.
#     - Performs other bulk find and replace: SCT_ prefix (only in specific sql queries).
#     """

#     #Find and replace for dates
#     bulk_replace_standard(sql_pub_fldr, configDates)

#     logging.info('Bulk find and replace complete for Dates.')

# #------------------------------------------------------------------------------------------------

# def bulk_replace_standard(sql_pub_fldr:str, configDates:class_Dates):
#     """
#     Function Actions:
#     - Bulk replaces all dates across all sql queries in the publication folder: Prev_PCDRel, MmmYY, YYYYMMDD.
#     """
    
#     path = os.listdir(sql_pub_fldr)
#     for content in path: 
#         #counter = 0
#         with open (PurePath(sql_pub_fldr, content), "r", encoding="latin") as file:
#             result = file.read()
#             #counter = result.count(MonYr)

#         result = result.replace('Prev_PCDRel', configDates.YYYYMMDD(configDates.PrevPCDrelDate))
#         result = result.replace('MmmYY', configDates.MmmYY)
#         result = result.replace('YYYYMMDD', configDates.YYYYMMDD(configDates.PCDreleaseDate))
        
#         with open(PurePath(sql_pub_fldr, content), "w", encoding="latin") as newFile:
#             newFile.write(result)
        
#     logging.info('Correct release dates replaced in SQL queries')

