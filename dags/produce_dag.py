import os, json
from datetime import timedelta, datetime
from airflow import DAG
from airflow.models.connection import Connection
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from contextlib import closing
from dotenv import load_dotenv
from google.cloud import pubsub_v1


load_dotenv()

value_used=os.path.join(os.getenv('FOLDER_USED'), 'used_date.json')
os.environ['GOOGLE_APPLICATION_CREDENTIALS']=os.getenv('GOOGLE_JSON')

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(
    os.getenv('GCP_PROJECT_ID'), 
    os.getenv('GCP_TOPIC_ID'))

def rewrite_json_used(value_insert:list) -> None:
    """
    Function which writes and rewrites the json value
    """
    with open(value_used, 'w+') as json_new:
        json.dump(value_insert, json_new, indent=4, sort_keys=True, default=str)

def _send_data_gcp(ti): 
    """
    Function which is dedicated to send the data to the airflow
    """
    value_sets = ti.xcom_pull(task_ids=['snowflake'])[0]
    
    for value_set in value_sets:
        player_id, device_id, install_date, client_id, app_name, country = value_set
        publisher.publish(
            topic_path, 
            str(
                {
                "player_id": player_id,
                "device_id": device_id,
                "install_date": install_date,
                "client_id": client_id,
                "app_name": app_name,
                "country": country
                }
            ).encode('UTF-8'),
            origin="python-sample", 
            username="gcp")
    print('Successful execution Pub/Sub')
    print('---------------------------------------------------------------------')
    
def developed_check() -> bool:
    """
    Function which is dedicated to check presence the beginning of the resending
    """
    value_return = os.path.exists(value_used) and os.path.isfile(value_used)
    if not value_return:
        return value_return
    return os.path.getsize(value_used) > 0
    
def get_datetime_used(ti):
    """
    Function which is dedicate to use the
    """
    datetime_now = datetime.now()
    value_new = ti.xcom_pull(task_ids=['snowflake_basic'])[0]
    value_min = value_new[0].get('MIN(INSTALL_DATE)')
    value_max = value_new[0].get('MAX(INSTALL_DATE)')
    
    value_min = value_min \
            if value_min and isinstance(value_min, datetime) else datetime_now   
    value_max = value_max \
            if value_max and isinstance(value_max, datetime) else datetime_now  
    
    value_presented, value_calculated = [], []
        
    if not developed_check():
        os.path.exists(os.getenv("FOLDER_USED")) or os.mkdir(os.getenv('FOLDER_USED'))
    else:
        with open(value_used, 'r') as json_old:
            for f in json.load(json_old):
                if f[1] >= 0:
                    value_presented.append(f[0])
                    value_calculated.append(f)
    value_insert = [value_min + timedelta(seconds=x) 
        for x in range(int((value_max - value_min).total_seconds()))]

    value_insert = [str(f) for f in value_insert if str(f) not in value_presented]
    
    value_insert = sorted([
        [f, -2] if k < int(os.getenv('LIMIT')) else [f, -1] 
        for k, f in enumerate(value_insert)], key= lambda f: f[1])

    value_insert.extend(value_calculated)
    
    rewrite_json_used(value_insert)
    return value_insert[:int(os.getenv('LIMIT'))]

def get_values_insert(ti):
    """
    Function for the sending the values 
    """
    value_result = []
    value_dates = ti.xcom_pull(task_ids=['datetime_json'])[0]
    
    with open(value_used, 'r') as values_json:
            value_required = json.load(values_json)

    with closing(
        SnowflakeHook(
            snowflake_conn_id=os.getenv('CONNECTION_ID')
            ).get_conn()) as conn:
        with closing(
            conn.cursor()) as cur:
            for value_date, _ in value_dates:
                value_result.extend(
                    cur.execute(
                        f"""SELECT PLAYER_ID, 
                            DEVICE_ID, 
                            INSTALL_DATE, 
                            CLIENT_ID, 
                            APP_NAME, 
                            COUNTRY 
                        FROM TEST_EVENTS WHERE INSTALL_DATE=%s""", 
                        [value_date]).fetchall()
                    )
                count = cur.execute(
                            f"SELECT COUNT(*) FROM TEST_EVENTS WHERE INSTALL_DATE=%s", 
                            [value_date]).fetchone()
                count = count[0] if count else 0
                value_required.remove([value_date, -2])
                value_required.append([value_date, count])
                rewrite_json_used(value_required)
    return value_result

with DAG(
    "produce_dag", 
    start_date=datetime.now(), 
    schedule_interval=timedelta(minutes=4), 
    catchup=False) as dag:

    snowflake_basic = SnowflakeOperator(
        task_id='snowflake_basic',
        sql="SELECT MIN(INSTALL_DATE), MAX(INSTALL_DATE) FROM TEST_EVENTS;",
        snowflake_conn_id=os.getenv('CONNECTION_ID')
    )

    datetime_json = PythonOperator(
        task_id = 'datetime_json',
        python_callable = get_datetime_used
    )

    snowflake = PythonOperator(
        task_id='snowflake',
        python_callable = get_values_insert
    )

    pub_sub = PythonOperator(
        task_id = 'pub_sub',
        python_callable = _send_data_gcp
    )

    snowflake_basic >> datetime_json >> snowflake >> pub_sub