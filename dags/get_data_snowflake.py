import os
import sys
import json
# import asyncio
# import pandas as pd
import datetime
from dotenv import load_dotenv
import snowflake.connector


load_dotenv()

class SnowflakeData:
    """
    class which is dedicated to check this values of the snowflake
    """
    def __init__(self, value_limit:int=5) -> None:
        self.folder_base = os.getcwd()
        self.folder_snow = 'snowflake_data'
        self.folder_used = os.path.join(
            self.folder_base, 
            self.folder_snow
            )
        self.value_used = os.path.join(
            self.folder_used,
            'used_date.json'
        )
        self.connection = snowflake.connector.connect(
            user=os.getenv('USERS'),
            password=os.getenv('PASSWORD'),
            account=os.getenv('ACCOUNT'),
            warehouse=os.getenv('WAREHOUSE'),
            database=os.getenv('DATABASE'),
            schema=os.getenv('SCHEMA')
            )
        self.cursor = self.connection.cursor()
        self.limit=value_limit

    def close(self) -> None:
        self.connection.close()

    @staticmethod
    def develop_folder(value_folder:str) -> None:
        os.path.exists(value_folder) or os.mkdir(value_folder)

    def developed_check(self) -> bool:
        """
        Method which is dedicated to check presence the beginning of the resending
        Input:  None
        Output: we checked all previously used values
        """
        value_return = os.path.exists(self.folder_used) and os.path.isdir(self.folder_used)
        if not value_return:
            return value_return
        value_return = os.path.exists(self.value_used) and os.path.isfile(self.value_used)
        if not value_return:
            return value_return
        return os.path.getsize(self.value_used) > 0

    @staticmethod
    def get_time(value_query:set, time_now:datetime.datetime) -> datetime.datetime:
        """
        Static method which is dedicated to develop values of the selected 
        """
        return value_query[0] \
            if value_query[0] and isinstance(value_query[0], datetime.datetime) else time_now

    def rewrite_json_used(self, value_insert:list) -> None:
        """
        Method which writes and rewrites the json value
        Input:  value_insert = inserted list to the json
        Output: file was written
        """
        with open(self.value_used, 'w+') as json_new:
            json.dump(value_insert, json_new, indent=4, sort_keys=True, default=str)

    def develop_datetime(self, value_check:bool=False) -> None:
        """
        Method which is dedicated to produce values of the datetime
        Input:  value_check = check on which value it was ended
        Output: we developed file with the fully dedicated values
        """
        datetime_now = datetime.datetime.now()
        value_min = self.cursor.execute("SELECT MIN(INSTALL_DATE) FROM TEST_EVENTS;").fetchone()
        value_max = self.cursor.execute("SELECT MAX(INSTALL_DATE) FROM TEST_EVENTS;").fetchone()
        value_min = self.get_time(value_min, datetime_now)   
        value_max = self.get_time(value_max, datetime_now)  
        value_presented, value_calculated = [], []
            
        if not self.developed_check():
            self.develop_folder(self.folder_used)
        else:
            with open(self.value_used, 'r') as json_old:
                for f in json.load(json_old):
                    if f[1] >= 0:
                        value_presented.append(f[0])
                        value_calculated.append(f)
        value_insert = [value_min + datetime.timedelta(seconds=x) 
            for x in range(int((value_max - value_min).total_seconds()))]
    
        value_insert = [str(f) for f in value_insert if str(f) not in value_presented]
        
        if not value_check:
            value_insert = [
                [f, -1] for f in value_insert]
        else:
            value_insert = sorted([
                [f, -2] if k < self.limit else [f, -1] 
                for k, f in enumerate(value_insert)], key= lambda f: f[1])

        value_insert.extend(value_calculated)
            
        self.rewrite_json_used(value_insert)
        return value_insert[:self.limit]
        
    def develop_values(self) -> list:
        """
        Method which is dedicated to return all parsed values for the giving 
        Input:  None
        Output: list with all possible values to sget them from the Snowflake
        """
        value_use = True
        value_result = []
        value_datetime = self.develop_datetime(value_use)
        
        with open(self.value_used, 'r') as values_json:
            value_required = json.load(values_json)

        for date, count in value_datetime:
            try:
                value_result.extend(self.cursor.execute(
                        f"""
                        SELECT PLAYER_ID, 
                            DEVICE_ID, 
                            INSTALL_DATE, 
                            CLIENT_ID, 
                            APP_NAME, 
                            COUNTRY 
                        FROM TEST_EVENTS WHERE INSTALL_DATE=%s""", 
                        [date]).fetchall()
                    )
                count = self.cursor.execute(
                            f"SELECT COUNT(*) FROM TEST_EVENTS WHERE INSTALL_DATE=%s", 
                            [date]).fetchone()
                count = count[0] if count else 0
                value_required.remove([date, -2]) if value_use else value_required.remove([date, -1])
                value_required.append([date, count])
                self.rewrite_json_used(value_required)
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(f'We faced problems with getting data with this timestamp: {date}')
                print(exc_type, fname, exc_tb.tb_lineno)
                print(f'Exception: {e}')
                print('========================================================================')
        return value_result


# if __name__ == '__main__':
    # a = SnowflakePythonTesting()
    # print(a.develop_values())
    # print('cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc')
    # a.close()