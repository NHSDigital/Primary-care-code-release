import pandas as pd
import sqlalchemy as sa
from sqlalchemy.sql import text
import logging
from pathlib import PurePath
from datetime import datetime
from dateutil.relativedelta import relativedelta


from src.utils.databases import class_UKSNOMEDCT_dbs
from src.utils.connection_fxns import sa_connect

class class_Dates:
    """
    Class Actions:
    - Defines all dates neede throughout the code as properties of the class.
    - Contains formatting methods for properties. 
    - Options for formats include: 'dd Month YYYY','YYYYMMDD','YYYY','MmmYY','dd.mm.yy'
    """
    def __init__(self, dates:dict):
        #defining PROPERTIES of the class
        try:
            self.MmmYY = dates['MmmYY']
        except KeyError:
            logging.critical('MmmYY has not been defined. Please check the config file.')
            raise KeyError('MmmYY has not been defined. Please check the config file.')
        logging.info(f"Using MmmYY: {self.MmmYY}")

        try:
            self.UKreleaseDate = dates['UKreleaseDate']
        except KeyError:
            logging.critical('UKreleaseDate has not been defined. Please check the config file.')
            raise KeyError('UKreleaseDate has not been defined. Please check the config file.')
        logging.info(f"Using UK release date: {self.UKreleaseDate}")

        try:
            self.PrevUKrelDate = dates['PrevUKrelDate']
        except KeyError:
            logging.critical('PrevUKrelDate has not been defined. Please check the config file.')
            raise KeyError('PrevUKrelDate has not been defined. Please check the config file.')
        logging.info(f"Using previous code release, UK release date: {self.PrevUKrelDate}")

        try:
            self.PCDreleaseDate = dates['PCDreleaseDate']
        except KeyError:
            logging.critical('PCDreleaseDate has not been defined. Please check the config file.')
            raise KeyError('PCDreleaseDate has not been defined. Please check the config file.')
        logging.info(f"Using PCD release date: {self.PCDreleaseDate}")

        try:
            self.PrevPCDrelDate = dates['PrevPCDrelDate']
        except KeyError:
            logging.critical('PrevPCDrelDate has not been defined. Please check the config file.')
            raise KeyError('PrevPCDrelDate has not been defined. Please check the config file.')
        logging.info(f"Using previous PCD release date: {self.PrevPCDrelDate}")

        for date in [self.UKreleaseDate, self.PrevUKrelDate, self.PCDreleaseDate, self.PrevPCDrelDate]:
            assert type(date) == str, f"{str(date)} is the wrong format. Please check the config file."
            assert len(date) == 8, f"{str(date)} is too many/few characters. Please check the config file."
        assert len(self.MmmYY) == 5, "MmmYY is too many/few characters. Please check the config file."

        self.INTreleaseDate = None  # found from module dependency dates query, defined later
        self.UKdrugsDate = None     # found from module dependency dates query, defined later
        self.PathologyDate = None   # found from module dependency dates query, defined later
        self.MmmYY_prev = None      # found from module dependency dates query, defined later
        
    #these are METHODS of the class
    def ddMonthYYYY(self, date):
        """
        Method Actions:
        - Takes date in string format 'YYYYMMDD'.
        - Returns date in string format 'dd Month YYYY'.
        """
        assert type(date) == str, f"date given was type {type(date)}, expecting a string"
        assert len(date) == 8, f"Incorrect date format: {date}. Please check config file."
        try:
            date = datetime.strptime(date, '%Y%m%d')
        except ValueError:
            logging.error(f"Incorrect date format: {date}. Please check config file.")
            raise ValueError(f"Incorrect date format: {date}. Please check config file.")
        return date.strftime('%d %B %Y')
    
    def YYYYMMDD(self, date):
        """
        Method Actions:
        - Takes date in string format 'YYYYMMDD'.
        - Returns date in string format 'YYYYMMDD'.
        """
        assert type(date) == str, f"date given was type {type(date)}, expecting a string"
        assert len(date) == 8, f"Incorrect date format: {date}. Please check config file."
        try:
            date = datetime.strptime(date, '%Y%m%d')
        except ValueError:
            logging.error(f"Incorrect date format: {date}. Please check config file.")
            raise ValueError(f"Incorrect date format: {date}. Please check config file.")
        return date.strftime('%Y%m%d')
    
    def YYYY(self, date):
        """
        Method Actions:
        - Takes date in string format 'YYYYMMDD'.
        - Returns date in string format 'YYYY'.
        """
        assert type(date) == str, f"date given was type {type(date)}, expecting a string"
        assert len(date) == 8, f"Incorrect date format: {date}. Please check config file."
        try:
            date = datetime.strptime(date, '%Y%m%d')
        except ValueError:
            logging.error(f"Incorrect date format: {date}. Please check config file.")
            raise ValueError(f"Incorrect date format: {date}. Please check config file.")
        return date.strftime('%Y')
    
    def MonYY(self, date):
        """
        Method Actions:
        - Takes date in string format 'YYYYMMDD'.
        - Returns date in string format 'MonYY'.
        """
        assert type(date) == str, f"date given was type {type(date)}, expecting a string"
        assert len(date) == 8, f"Incorrect date format: {date}. Please check config file."
        try:
            date = datetime.strptime(date, '%Y%m%d')
        except ValueError:
            logging.error(f"Incorrect date format: {date}. Please check config file.")
            raise ValueError(f"Incorrect date format: {date}. Please check config file.")
        return date.strftime('%b%y')
    
    def ddmmyy(self, date):
        """
        Method Actions:
        - Takes date in string format 'YYYYMMDD'.
        - Returns date in string format 'ddmmyy'.
        """
        assert type(date) == str, f"date given was type {type(date)}, expecting a string"
        assert len(date) == 8, f"Incorrect date format: {date}. Please check config file."
        try:
            date = datetime.strptime(date, '%Y%m%d')
        except ValueError:
            logging.error(f"Incorrect date format: {date}. Please check config file.")
            raise ValueError(f"Incorrect date format: {date}. Please check config file.")
        return date.strftime('%d.%m.%y')
    
    def YmdDashes(self, date):
        """
        Method Actions:
        - Takes date in string format 'YYYYMMDD'.
        - Returns date in string format 'YYYY-MM-DD'.
        """
        assert type(date) == str, f"date given was type {type(date)}, expecting a string"
        assert len(date) == 8, f"Incorrect date format: {date}. Please check config file."
        try:
            date = datetime.strptime(date, '%Y%m%d')
        except ValueError:
            logging.error(f"Incorrect date format: {date}. Please check config file.")
            raise ValueError(f"Incorrect date format: {date}. Please check config file.")
        return date.strftime('%Y-%m-%d')
    
    def prev_MmmYY(self, UKSNOMEDCT:class_UKSNOMEDCT_dbs):
        """
        Method Actions:
        - Finds the name of the UK SNOMED CT database that was used in the previous PCD SNOMED Release.
        - Saves the previous MmmYY as a property of the class.
        """
        #define previous MmmYY as current as a starting point
        MmmYY_prev = self.MmmYY
        last = MmmYY_prev
        found_prev = False

        database_name = UKSNOMEDCT.live
        

        #loop back over the last 12 months to find the database containing a GPData_Cluster_refset_(previous date) table
        for r in range(16):
            database_name = database_name.replace(last, MmmYY_prev)
            try:
                # connect to MmmYY_prev UK SNOMED CT database, starting with the current, looping -1 month each time
                engine = sa_connect(server=UKSNOMEDCT.server, database=database_name)

                # table is in UK SNOMED CT database, so connection above will bring it back in INFORMATION_SCHEMA.TABLES list
                with engine.connect() as conn:
                    print('checking prevPCDrel date runs = ',database_name,  {self.YYYYMMDD(self.PrevPCDrelDate)})
                    query = conn.execute(text(f'''
                                            SET NOCOUNT ON
                                            SELECT * FROM INFORMATION_SCHEMA.TABLES
                                            WHERE TABLE_NAME = 'GPData_Cluster_refset_1000230_{self.YYYYMMDD(self.PrevPCDrelDate)}'
                                            '''))
                    GPtables = pd.DataFrame(query.fetchall())
            
            #thrown up if sql connection (engine) can't be made (database doesn't exist)
            except sa.exc.OperationalError:
                last = MmmYY_prev
                MmmYY_prev = (datetime.strptime(MmmYY_prev, '%b%y') - relativedelta(months = 1)).strftime('%b%y')
                continue 
            
            #thrown up if sql query errors (table doesn't exist)
            except sa.exc.ProgrammingError:
                last = MmmYY_prev
                MmmYY_prev = (datetime.strptime(MmmYY_prev, '%b%y') - relativedelta(months = 1)).strftime('%b%y')
                continue 
            
            if len(GPtables) > 0:
                found_prev = True
                break
            else:
                #if table contains no content
                last = MmmYY_prev
                MmmYY_prev = (datetime.strptime(MmmYY_prev, '%b%y') - relativedelta(months = 1)).strftime('%b%y')
        
        #error out if previous MmmYY cannot be found
        assert found_prev == True, f"Previous MmmYY date could not be found in the last 12 months. Please check that the UK SNOMED CT database from the previous code release contains a table with the name: GPData_Cluster_refset_1000230_{self.YYYYMMDD(self.PrevPCDrelDate)}."
            
        self.MmmYY_prev = MmmYY_prev

        logging.info(f"Using previous PCD MmmYY: {self.MmmYY_prev}")

        return self.MmmYY_prev
    
    def INT_Drug_Path(self, MD_pd_actual_data:pd.DataFrame):
        """
        Method Actions:
        - Finds the international, drug, and pathology release dates from the module dependency query table.
        - Saves the dates as a properties of the class.
        """
        #check the dataframe is in the expected format
        for col in ['ReleaseType', 'moduleId', 'sourceEffectiveTime']:
            assert col in MD_pd_actual_data.columns, f"{col} not in dataframe. Please check module dependency table."

        new_dates = {}
        for x in ['Drug', 'International', 'Pathology']:
            try: 
                date_str = MD_pd_actual_data.loc[(MD_pd_actual_data['ReleaseType'] == x) & (MD_pd_actual_data['sourceEffectiveTime'].isnull() == False)].iloc[0,2]
                new_dates[x] = date_str
            except IndexError:
                new_dates[x] = ''

        self.INTreleaseDate = new_dates['International']
        self.UKdrugsDate = new_dates['Drug']
        self.PathologyDate = new_dates['Pathology']

        logging.info(f"Using international release date: {self.INTreleaseDate}")
        logging.info(f"Using UK drugs release date: {self.UKdrugsDate}")
        logging.info(f"Using pathology release date: {self.PathologyDate} - this date may be nan, but that is okay and means there are no pathology codes in circuit.")

        return self.INTreleaseDate, self.UKdrugsDate, self.PathologyDate

    def save_down_dates(self, publishing_TRUD_fldr:PurePath):

        df = pd.DataFrame({"Value":[self.MmmYY,
                                    self.MmmYY_prev,
                                    self.UKreleaseDate,
                                    self.PrevUKrelDate,
                                    self.PCDreleaseDate,
                                    self.PrevPCDrelDate,
                                    self.INTreleaseDate,
                                    self.UKdrugsDate,
                                    self.PathologyDate]}, 
                            
                            index = ["MmmYY",
                                     "MmmYY_prev",
                                     "UKreleaseDate",
                                     "PrevUKrelDate",
                                     "PCDreleaseDate",
                                     "PrevPCDrelDate",
                                     "INTreleaseDate",
                                     "UKdrugsDate",
                                     "PathologyDate"])
        
        df.to_excel(PurePath(publishing_TRUD_fldr, "date_variables.xlsx"))
        logging.info(f"date_variables.xlsx saved to {publishing_TRUD_fldr}. This file will be used in future scripts.")
        return
    
    def dates_from_file(self, df:pd.DataFrame):
        
        message = "in date_variables.xlsx does not match config file. Please delete xlsx if out of date, or ammend config file."

        assert df.loc["MmmYY"][0]           == self.MmmYY, "MmmYY " + message
        assert df.loc["UKreleaseDate"][0]   == self.UKreleaseDate, "UKreleaseDate " + message
        assert df.loc["PrevUKrelDate"][0]   == self.PrevUKrelDate, "PrevUKrelDate " + message 
        assert df.loc["PCDreleaseDate"][0]  == self.PCDreleaseDate, "PCDreleaseDate " + message
        assert df.loc["PrevPCDrelDate"][0]  == self.PrevPCDrelDate, "PrevPCDrelDate " + message
        
        self.MmmYY_prev     = df.loc['MmmYY_prev'][0]
        self.INTreleaseDate = df.loc['INTreleaseDate'][0]
        self.UKdrugsDate    = df.loc['UKdrugsDate'][0]
        self.PathologyDate  = df.loc['PathologyDate'][0]

        logging.info(f"Using previous PCD MmmYY: {self.MmmYY_prev}.")
        logging.info(f"Using international release date: {self.INTreleaseDate}.")
        logging.info(f"Using UK drugs release date: {self.UKdrugsDate}.")
        logging.info(f"Using pathology release date: {self.PathologyDate} - this date may be nan, but that is okay and means there are no pathology codes in circuit.")
        
        return
    

  
