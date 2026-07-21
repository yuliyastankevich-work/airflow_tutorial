from airflow.sdk import dag, task
import os

@dag(
        dag_id = "second_dag"
)
def second_dag():

    @task.python
    def first_task():
        print("First task in the second DAG")
    
    @task.python
    def second_task():
        print("Second task in the second DAG")

    @task.python
    def third_task():
        print("Third task in the first DAG")
        os.makedirs(os.path.dirname('/opt/airflow/logs/data'), exist_ok = True)
        with open('/opt/airflow/logs/data/orch_2_file.txt', 'w') as f:
            f.write(f"The child DAG 2 was activated successfully")

    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third

second_dag()
