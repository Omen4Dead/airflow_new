# The DAG object; we'll need this to instantiate a DAG
from airflow.models.dag import DAG

# Operators;
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Utils;
from airflow.utils.dates import days_ago

# Other imports
from datetime import timedelta
import requests

default_args = {
    "owner": "Nick",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}


def hw():
    print('Hello Airflow from Python')


def req():
    print(requests.get('https://www.google.ru'))


with DAG(
    dag_id='my_first_dag',  # Название - должно совпадать с назвнием файла .py
    default_args=default_args,  # Словарь аргументов, созданный в 14 строке
    description='Мой первый DAG на курсе',  # Описание
    schedule_interval=None,  # Расписание запусков (можно в формате Cron-выражения)
    start_date=days_ago(2),  # Обязательно дата в прошлом
    tags=['my']  # тег - удобно для разбивки DAG на группы
) as dag:
    bash_first = BashOperator(
        task_id='bash_first',  # task_id должен быть уникальным в рамках одного DAG
        bash_command='echo Hello Airflow from Bash'
    )
    bash_last = BashOperator(
        task_id='bash_last',
        bash_command='echo Hello Airflow from Bash'
    )
    python_1 = PythonOperator(
        task_id='python_1',
        python_callable=hw  # передаем название функции, без вызова ()
    )
    python_2 = PythonOperator(
        task_id='python_2',
        python_callable=req
    )

    bash_first >> [python_1, python_2] >> bash_last
