import os
import sys
from dotenv import load_dotenv
from google.cloud import pubsub_v1

load_dotenv()
os.environ['GOOGLE_APPLICATION_CREDENTIALS']=os.getenv('GOOGLE_JSON')

class GcpData:
    """
    class which is dedicated to sending values to the gcp
    """
    def __init__(self) -> None:
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(
            os.getenv('GCP_PROJECT_ID'), 
            os.getenv('GCP_TOPIC_ID'))

    @staticmethod
    def develop_message(value_dict:dict, kwargs:dict) -> dict:
        """
        Static method which is dedicated to create dictionary from the
        Input:  value_dict = dictionary from the 
                **kwargs = all other arguments from the link parameter
        Output: one dictionary from the several elements
        """
        return str({
            **value_dict,
            **kwargs}).encode('UTF-8')

    def send_message_gcp(self, value_send:dict) -> None:
        """
        Method which is dedicated to send the pub/sub
        Input:  value_send = made value of the dictionary values
        Output: we developed values to it
        """
        return self.publisher.publish(
            self.topic_path, 
            value_send,
            origin="python-sample", 
            username="gcp")

    @staticmethod
    def get_dictionary(value_set:set) -> dict:
        """
        Static method which is dedicated to get dictionary values
        Input:  value_set = 1
        Output: dictionary from       
        """
        player_id, device_id, install_date, client_id, app_name, country = value_set
        return {
            "player_id": player_id,
            "device_id": device_id,
            "install_date": install_date,
            "client_id": client_id,
            "app_name": app_name,
            "country": country
        }

    def develop_message_group(self, value_sets:list, **kwargs:dict) -> None:
        """
        Method which is dedicated to make this group values
        Input:  value_set = list of set values which was previously got from the snowflake
                kwargs = all additional values to the arguments
        Output: we developed values to the sender
        """
        for value_set in value_sets:
            try:
                value_dict = self.get_dictionary(value_set)
                if not kwargs:
                    self.send_message_gcp(
                        str(value_dict).encode('UTF-8')
                    )
                else:
                    self.send_message_gcp(
                        self.develop_message(
                            value_dict,
                            kwargs
                        )
                    )
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(
                    f"We faced problems with getting data with gcp sending; Datetime: {value_dict.get('install_date')}")
                print(exc_type, fname, exc_tb.tb_lineno)
                print(f'Exception: {e}')
                print('========================================================================')