import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}

dag = DAG(
    dag_id = 'dag_datas_marry',
    default_args = args,
    schedule_interval = '@daily',
    dagrun_timeout = timedelta(minutes=60)
)

t1 = BashOperator(
    task_id='taks_datas_marry',
    bash_command='date',
    dag=dag
)

t2 = BashOperator(
    task_id='spleep_10s',
    bash_command='spleep 10',
    retries=3,
    dag=dag
)

t3 = BashOperator(
    task_id='saida',
    bash_command='date > /opt/airflow/outputs/date_output.text',
    retries=3,
    dag=dag
)

t1 >> [t2,t3]