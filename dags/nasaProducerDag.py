"""
#######################################################################
#Created by: Miguel Angel Chanchavac Alvarado
#Date created: 20240109
#overview: Processing real-time data about nasa satellite position
#utilizing Airflow, PySpark, Apache Kafka, S3, Athena
#######################################################################
"""

import pandas as pd
import datetime
from datetime import timedelta
from airflow.models import DAG, Variable
from airflow.operators.bash import BashOperator
import configparser

config = configparser.ConfigParser()
config.read('/Users/mangeluz/airflow/dags/csv/nasa.ini')

env = 'DEV'
section_path_dag = f'PATH_DAG_{env}'
FILE_CONFIG = config.get(section_path_dag, 'FILE_CONFIG')
DAG_PATH = config.get(section_path_dag, 'DAG_PATH')
FORMAT_FILE = config.get(section_path_dag, 'FORMAT_FILE')

section_dag = f'DAG_{env}'
DAG_ID = config.get(section_dag, 'DAG_ID')
POOL_MAIN = config.get(section_dag, 'POOL_MAIN')

path_file = f"{DAG_PATH}{FILE_CONFIG}.{FORMAT_FILE}"


def read_config(path_file: str):
    try:
        table_df_csd = pd.read_csv(path_file, sep='|')
        table_df_csd = table_df_csd.sort_values(by=['priority', 'item_id', 'seq'])
        return table_df_csd.to_dict('records')
    except Exception as e:
        print("Error tried to read config file: ".format(e))


table_list_dict = read_config(path_file)


class dummy_task:
    def __init__(self, year, month, day) -> None:
        self.year = year
        self.month = month
        self.day = day

    def dag(self, owner):
        try:
            args = {
                'owner': owner,
                'start_date': datetime.datetime(self.year, self.month, self.day),
                'depends_on_past': True,
                'wait_for_downstream': True,
            }

            return DAG(
                dag_id=DAG_ID,
                default_args=args,
                schedule=None,
                dagrun_timeout=timedelta(minutes=60),
            )
        except Exception as e:
            print("Error for build of DAG: ".format(e))

    def dummyTask(self, task_id, dag1) -> BashOperator:
        return BashOperator(
            task_id=task_id,
            # priority_weight = weight_var,
            bash_command='echo "{{ ts }}" && sleep 1',
            dag=dag1
        )

    def bashOperator(self, target, item_id, query, pool_main, dag1):
        try:
            return BashOperator(
                task_id=target + '_' + item_id,
                bash_command='python3' + ' ' + query,
                dag=dag1,
                pool=pool_main,
            )
        except Exception as e:
            print("Error Bash Operator Task: ".format(e))


dag_task = dummy_task(year=2024, month=1, day=9)
dag = dag_task.dag(owner='Airflow')

ta = dag_task.dummyTask(task_id='Start_Dag', dag1=dag)
tb = dag_task.dummyTask(task_id='End_Load_Libraries', dag1=dag)
tc = dag_task.dummyTask(task_id='Finish_Dag', dag1=dag)

for i in table_list_dict:
    if i['type_table'] == "PYTHON" and i['item_id'] == "T001.1":
        t1 = dag_task.bashOperator(target=i['target'], item_id=i['item_id'], query=i['query'],
                                   pool_main=POOL_MAIN,
                                   dag1=dag)
        ta >> t1 >> tb

    if i['type_table'] == "PYTHON" and i['item_id'] == "T001.2":
        t2 = dag_task.bashOperator(target=i['target'], item_id=i['item_id'], query=i['query'],
                                   pool_main=POOL_MAIN,
                                   dag1=dag)
        ta >> t2 >> tb

    if i['type_table'] == "PYTHON" and i['item_id'] == "T001.3":
        t3 = dag_task.bashOperator(target=i['target'], item_id=i['item_id'], query=i['query'],
                                   pool_main=POOL_MAIN,
                                   dag1=dag)
        ta >> t3 >> tb

    if i['type_table'] == "PYTHON" and i['item_id'] == "T001.4":
        t4 = dag_task.bashOperator(target=i['target'], item_id=i['item_id'], query=i['query'],
                                   pool_main=POOL_MAIN,
                                   dag1=dag)
        ta >> t4 >> tb

    if i['type_table'] == "PYTHON" and i['item_id'] == "T001.5":
        t5 = dag_task.bashOperator(target=i['target'], item_id=i['item_id'], query=i['query'],
                                   pool_main=POOL_MAIN,
                                   dag1=dag)
        ta >> t5 >> tb

    if i['type_table'] == "PYTHON" and i['item_id'] == "T001.6":
        t6 = dag_task.bashOperator(target=i['target'], item_id=i['item_id'], query=i['query'],
                                   pool_main=POOL_MAIN,
                                   dag1=dag)
        ta >> t6 >> tb

    if i['type_table'] == "PYTHON" and i['item_id'] == "T002.1":
        t7 = dag_task.bashOperator(target=i['target'], item_id=i['item_id'], query=i['query'],
                                   pool_main=POOL_MAIN,
                                   dag1=dag)
        tb >> t7 >> tc
