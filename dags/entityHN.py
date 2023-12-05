#######################################################################
#Created by: Miguel Angel Chanchavac Alvarado
#Date created: 20230712
#overview: 
#######################################################################
import pandas as pd
import datetime
from datetime import timedelta
import airflow
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

FILE_CONFIG = 'entityConfig'
DAG_PATH = '/Users/mangeluz/airflow/dags/csv/'
FORMAT_FILE = 'txt'
DAG_ID = 'ENTITIES_HN'
POOL_MAIN = 'default_pool'
PATH_VENV = '/Users/mangeluz/Documents/millicom/Honduras/pythonCode/entities/'

#----CSV read configuration
table_df_csd = pd.read_csv(DAG_PATH + '/' + FILE_CONFIG + '.' + FORMAT_FILE,sep='|')
table_df_csd = table_df_csd.sort_values(by=['priority', 'item_id', 'seq'])
table_list_dict = table_df_csd.to_dict('records')

def run_venv():
    import subprocess

    activate_cmd = PATH_VENV + 'venv/bin/activate'
    subprocess.run(['source', activate_cmd], shell=True)

args = {
    'owner': 'Airflow',
    'start_date': datetime.datetime(2023, 7, 13),
    'depends_on_past': True,
    'wait_for_downstream' : True,
    #'on_failure_callback': aws.task_fail_email_alert,
}

dag = DAG(
    dag_id=DAG_ID,
    default_args=args,
    #schedule_interval='50 11 * * *',
    dagrun_timeout=timedelta(minutes=60),
)

#--------------- Create dummy task ----------------
######################################################
######################################################
ta = BashOperator(
    task_id='Start_dag',
    #priority_weight = weight_var,
    bash_command='echo "{{ ts }}" && sleep 1',
    dag=dag,
    )

tb = BashOperator(
    task_id='End_load_lib',
    #priority_weight = weight_var,
    bash_command='echo "{{ ts }}" && sleep 1',
    dag=dag,
    )

tc = BashOperator(
    task_id='End_entities',
    #priority_weight = weight_var,
    bash_command='echo "{{ ts }}" && sleep 1',
    dag=dag,
    )

td = BashOperator(
    task_id='End_kafka',
    #priority_weight = weight_var,
    bash_command='echo "{{ ts }}" && sleep 1',
    dag=dag,
    )

t99 = BashOperator(
    task_id='End_dag',
    #priority_weight = weight_var,
    bash_command='echo "{{ ts }}" && sleep 1',
    dag=dag,
    )


t0 = PythonOperator(
    task_id='run_venv',
    #priority_weight = weight_var,
    python_callable=run_venv,
    dag=dag,
    )

for i in table_list_dict:
    ######Load all libreries and dependencies
    if i['type_table'] == "PYTHON" and i['item_id'] == "T001.1":
        t1 = BashOperator(
            task_id = i['target']+'_'+i['item_id'],
            bash_command='python3'+' '+i['query'],
            dag=dag,
            pool=POOL_MAIN,
        )
        ta >> t0 >> t1
    
    if i['type_table'] == "PYTHON" and i['item_id'] == "T001.2":
        t2 = BashOperator(
            task_id = i['target']+'_'+i['item_id'],
            bash_command='python3'+' '+i['query'],
            dag=dag,
            pool=POOL_MAIN,
        )
        ta >> t0 >> t2 >> tb

    if i['type_table'] == "PYTHON" and i['item_id'] == "T001.3":
        t3 = BashOperator(
            task_id = i['target']+'_'+i['item_id'],
            bash_command='python3'+' '+i['query'],
            dag=dag,
            pool=POOL_MAIN,
        )
        ta >> t0 >> t3 >> tb

    if i['type_table'] == "PYTHON" and i['item_id'] == "T001.4":
        t4 = BashOperator(
            task_id = i['target']+'_'+i['item_id'],
            bash_command='python3'+' '+i['query'],
            dag=dag,
            pool=POOL_MAIN,
        )
        ta >> t0 >> t4 >> tb
    
    if i['type_table'] == "PYTHON" and i['item_id'] == "T001.5":
        t5 = BashOperator(
            task_id = i['target']+'_'+i['item_id'],
            bash_command='python3'+' '+i['query'],
            dag=dag,
            pool=POOL_MAIN,
        )
        t1 >> t5 >> tb

    ######Working all entities 
    if i['type_table'] == "PYTHON" and i['item_id'] == "T001.6":
        t6 = BashOperator(
            task_id = i['target']+'_'+i['item_id'],
            bash_command='python3'+' '+i['query'],
            dag=dag,
            pool=POOL_MAIN,
        )
        tb >> t6 >> tc

    if i['type_table'] == "PYTHON" and i['item_id'] == "T001.7":
        t7 = BashOperator(
            task_id = i['target']+'_'+i['item_id'],
            bash_command='python3'+' '+i['query'],
            dag=dag,
            pool=POOL_MAIN,
        )
        tb >> t7 >> tc

    if i['type_table'] == "PYTHON" and i['item_id'] == "T001.8":
        t8 = BashOperator(
            task_id = i['target']+'_'+i['item_id'],
            bash_command='python3'+' '+i['query'],
            dag=dag,
            pool=POOL_MAIN,
        )
        tb >> t8 >> tc

    if i['type_table'] == "PYTHON" and i['item_id'] == "T001.9":
        t9 = BashOperator(
            task_id = i['target']+'_'+i['item_id'],
            bash_command='python3'+' '+i['query'],
            dag=dag,
            pool=POOL_MAIN,
        )
        tc >> t9 >> td
    
    if i['type_table'] == "PYTHON" and i['item_id'] == "T001.10":
        t10 = BashOperator(
            task_id = i['target']+'_'+i['item_id'],
            bash_command='python3'+' '+i['query'],
            dag=dag,
            pool=POOL_MAIN,
        )
        tc >> t10 >> td
    
    if i['type_table'] == "PYTHON" and i['item_id'] == "T001.11":
        t11 = BashOperator(
            task_id = i['target']+'_'+i['item_id'],
            bash_command='python3'+' '+i['query'],
            dag=dag,
            pool=POOL_MAIN,
        )
        tc >> t11 >> td
        td >> t99
