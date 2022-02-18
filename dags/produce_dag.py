import os, subprocess
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from get_data_snowflake import SnowflakeData
from send_data_gcp import GcpData


def _get_task_data_snowflake():
    """
    Function which is dedicated to get the data from the snowflake
    """
    try:
        a = SnowflakeData()
        ret = a.develop_values()
        ret = [(one, two, str(three), four, five, six) for one, two, three, four, five, six in ret]
        print('Successful execution Snowflake')
        print('---------------------------------------------------------------------')
        return res
    except Exception as e:
        a.close()
        print(f'Exception: {e}')
        print('========================================================================')
        return res_bad
    
def _get_res_one():
    return 1

def _get_res_zero():
    return 0

def _get_task_send_gcp(ti):
    """
    Function to send all presented values to the pub/sub
    """
    try:
        
        value_set = ti.xcom_pull(task_ids=[
            'snowflake'
        ])
        # print(value_set)
        # print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
        v = GcpData()
        v.develop_message_group(value_set[0])
        print('Successful execution Pub/Sub')
        print('---------------------------------------------------------------------')
    except Exception as e:
        print(f'Exception: {e}')
        print('========================================================================')
    finally:
        return 'res'

with DAG(
    "produce_gan", 
    start_date=datetime.now(), 
    schedule_interval=timedelta(minutes=4), 
    catchup=False) as dag:

    snowflake = PythonOperator(
        task_id = 'snowflake',
        python_callable = _get_task_data_snowflake
    )

    pub_sub = BranchPythonOperator(
        task_id = 'pub_sub',
        python_callable = _get_task_send_gcp
    )

    res = PythonOperator(
        task_id='res',
        python_callable= _get_res_one
    )

    res_bad = PythonOperator(
        task_id='res_bad',
        python_callable= _get_res_zero
    )

    # list_python_packages_operator >> 
    snowflake >> pub_sub