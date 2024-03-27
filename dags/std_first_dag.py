import random
import datetime as dt

from airflow.models import DAG
from airflow.operators.python import PythonOperator, get_current_context
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'Nick',
    'start_date': dt.datetime(2024, 3, 10),
    'retries': 2,
    'retry_delay': dt.timedelta(seconds=10),
}


def random_dice():

    val = random.randint(1, 6)
    if val % 2 != 0:
        raise ValueError(f'Odd {val}')


def even_only():
    context = get_current_context()
    execution_date = context['logical_date']

    if execution_date.day % 2 != 0:
        raise ValueError(f'Odd day: {execution_date}')


with DAG(dag_id='std_first_dag',
         schedule_interval='@daily',
         default_args=default_args,
         tags=['my']) as dag:

    dice = PythonOperator(
        task_id='random_dice',
        python_callable=random_dice,
        dag=dag,
    )

    even_only = PythonOperator(
        task_id='even_only',
        python_callable=even_only,
        dag=dag,
    )

    dummy = EmptyOperator(
        task_id='dummy_task',
        dag=dag
    )

    dice >> even_only >> dummy
