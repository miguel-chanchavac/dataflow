"""
#######################################################################
#Created by: Miguel Angel Chanchavac Alvarado
#Date created: 20240102
#overview: Processing data about FYR company
#######################################################################
"""

import pandas as pd
import datetime
from datetime import timedelta
from airflow.models import DAG, Variable
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import configparser
from airflow.hooks.base import BaseHook


config = configparser.ConfigParser()
config.read('/Users/mangeluz/airflow/dags/csv/fyrConfig.ini')

env = 'DEV'
section_path_dag = f'PATH_DAG_{env}'
FILE_CONFIG = config.get(section_path_dag, 'FILE_CONFIG')
DAG_PATH = config.get(section_path_dag, 'DAG_PATH')
FORMAT_FILE = config.get(section_path_dag, 'FORMAT_FILE')
SQL_PATH = config.get(section_path_dag, 'SQL_PATH')

section_dag = f'DAG_{env}'
DAG_ID = config.get(section_dag, 'DAG_ID_DASH')
POOL_MAIN = config.get(section_dag, 'POOL_MAIN')

path_file = f"{DAG_PATH}{FILE_CONFIG}.{FORMAT_FILE}"

section_sql_path = f'SQL_PATH_{env}'
postgres_conn = BaseHook.get_connection('fyrFinanciero')
SCHEMA = config.get(section_sql_path, 'SCHEMA')
DATABASE = config.get(section_sql_path, 'DATABASE')

sql_dir = Variable.get("sql_dir", default_var=SQL_PATH)


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
                bash_command=query,
                dag=dag1,
                pool=pool_main,
            )
        except Exception as e:
            print("Error Bash Operator Task: ".format(e))

    def postgresOperator2(self, target, item_id, query, pool_main, dag1):
        try:
            return PostgresOperator(
                task_id=target + '_' + item_id,
                postgres_conn_id=postgres_conn.conn_id,
                sql=query,
                dag=dag1,
                pool=pool_main,
            )
        except Exception as e:
            print("Error Postgres Operator2 Task: ".format(e))

    def postgresOperator(self, target, item_id, query, pool_main, dag1):
        try:
            return SQLExecuteQueryOperator(
                task_id=target + '_' + item_id,
                conn_id=postgres_conn.conn_id,
                sql=query,
                hook_params={SCHEMA: DATABASE},
                dag=dag1,
                pool=pool_main,
            )
        except Exception as e:
            print("Error Postgres Operator Task: ".format(e))


dag_task = dummy_task(year=2024, month=1, day=2)
dag = dag_task.dag(owner='Airflow')

ta = dag_task.dummyTask(task_id='Start_dag', dag1=dag)
tb = dag_task.dummyTask(task_id='End_load_stage', dag1=dag)
tc = dag_task.dummyTask(task_id='End_History', dag1=dag)
td = dag_task.dummyTask(task_id='Start_EF', dag1=dag)
te = dag_task.dummyTask(task_id='End_EF', dag1=dag)
tf = dag_task.dummyTask(task_id='End_tables', dag1=dag)
tg = dag_task.dummyTask(task_id='End_dag', dag1=dag)

for i in table_list_dict:
    if i['type_table'] == "PYTHON" and i['item_id'] == "T001.1":
        t1 = dag_task.bashOperator(target=i['target'], item_id=i['item_id'], query='python ' + i['query'],
                                   pool_main=POOL_MAIN,
                                   dag1=dag)
        ta >> t1

    if i['type_table'] == "PYTHON" and i['item_id'] == "T001.2":
        t2 = dag_task.bashOperator(target=i['target'], item_id=i['item_id'], query='python ' + i['query'],
                                   pool_main=POOL_MAIN,
                                   dag1=dag)
        t1 >> t2 >> tb

    if i['type_table'] == "SQL" and i['item_id'] == "T002.1":
        t3 = dag_task.postgresOperator(target=i['target'], item_id=i['item_id'], query=i['query'],
                                       pool_main=POOL_MAIN,
                                       dag1=dag)
        tb >> t3 >> tc >> td

    if i['type_table'] == "SQL" and i['item_id'] == "T003.1":
        t4 = dag_task.postgresOperator(target=i['target'], item_id=i['item_id'], query=i['query'], pool_main=POOL_MAIN,
                                       dag1=dag)
        td >> t4 >> te >> tf

    if i['type_table'] == "SQL" and i['item_id'] == "T003.2":
        t5 = dag_task.postgresOperator(target=i['target'], item_id=i['item_id'], query=i['query'], pool_main=POOL_MAIN,
                                       dag1=dag)
        t4 >> t5 >> te >> tf

    if i['type_table'] == "SQL" and i['item_id'] == "T003.3":
        t6 = dag_task.postgresOperator(target=i['target'], item_id=i['item_id'], query=i['query'], pool_main=POOL_MAIN,
                                       dag1=dag)
        t4 >> t6 >> te >> tf

    if i['type_table'] == "SQL" and i['item_id'] == "T003.4":
        t7 = dag_task.postgresOperator(target=i['target'], item_id=i['item_id'], query=i['query'], pool_main=POOL_MAIN,
                                       dag1=dag)
        t4 >> t7 >> te >> tf

    if i['type_table'] == "SQL" and i['item_id'] == "T003.5":
        t8 = dag_task.postgresOperator(target=i['target'], item_id=i['item_id'], query=i['query'], pool_main=POOL_MAIN,
                                       dag1=dag)
        t4 >> t8 >> te >> tf

    if i['type_table'] == "SQL" and i['item_id'] == "T003.6":
        t9 = dag_task.postgresOperator(target=i['target'], item_id=i['item_id'], query=i['query'], pool_main=POOL_MAIN,
                                       dag1=dag)
        t4 >> t9 >> te >> tf

    if i['type_table'] == "SQL" and i['item_id'] == "T003.7":
        t10 = dag_task.postgresOperator(target=i['target'], item_id=i['item_id'], query=i['query'], pool_main=POOL_MAIN,
                                        dag1=dag)
        t4 >> t10 >> te >> tf

    if i['type_table'] == "SQL" and i['item_id'] == "T003.8":
        t11 = dag_task.postgresOperator(target=i['target'], item_id=i['item_id'], query=i['query'], pool_main=POOL_MAIN,
                                        dag1=dag)
        t4 >> t11 >> te >> tf

    if i['type_table'] == "SQL" and i['item_id'] == "T003.9":
        t12 = dag_task.postgresOperator(target=i['target'], item_id=i['item_id'], query=i['query'], pool_main=POOL_MAIN,
                                        dag1=dag)
        t4 >> t12 >> te >> tf

    if i['type_table'] == "SQL" and i['item_id'] == "T003.10":
        t13 = dag_task.postgresOperator(target=i['target'], item_id=i['item_id'], query=i['query'], pool_main=POOL_MAIN,
                                        dag1=dag)
        tc >> t13 >> tf

    if i['type_table'] == "SQL" and i['item_id'] == "T003.11":
        t14 = dag_task.postgresOperator(target=i['target'], item_id=i['item_id'], query=i['query'], pool_main=POOL_MAIN,
                                        dag1=dag)
        tc >> t14 >> tf

    if i['type_table'] == "SQL" and i['item_id'] == "T003.12":
        t15 = dag_task.postgresOperator(target=i['target'], item_id=i['item_id'], query=i['query'], pool_main=POOL_MAIN,
                                        dag1=dag)
        tc >> t15 >> tf

    if i['type_table'] == "SQL" and i['item_id'] == "T003.13":
        t16 = dag_task.postgresOperator(target=i['target'], item_id=i['item_id'], query=i['query'], pool_main=POOL_MAIN,
                                        dag1=dag)
        tc >> t16 >> tf
        tf >> tg
