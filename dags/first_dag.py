from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
def first_function_execute(**context):
    print('First Function Execute')
    context['ti'].xcom_push (key='mykey', value='First Func Says Hllo')
    return 'First Function Execute'
def second_function_execute(**context):
    instance = context.get('ti').xcom_pull(key='mykey')
    print(f'I am second function got value {instance} from second function')
    return "hello world" + instance

with DAG(
    dag_id="first_dag",
    schedule_interval="@daily",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": datetime(2020, 11, 1),
    },
    catchup=False) as f:
    first_function_execute = PythonOperator(
        task_id="first_function_execute",
        python_callable=first_function_execute,
                op_kwargs={"name":"HAMZA ANALYST"},
                            provide_context=True
    )
    second_function_execute = PythonOperator(
        task_id="second_function_execute",
        python_callable=second_function_execute,
        provide_context=True
        )
first_function_execute >> second_function_execute