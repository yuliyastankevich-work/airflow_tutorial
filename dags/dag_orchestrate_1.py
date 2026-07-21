from airflow.sdk import dag, task
import os

@dag(
        dag_id = "first_dag"
)
def first_dag():

    @task.python
    def first_task():
        print("First task in the first DAG")
    
    @task.python
    def second_task():
        print("Second task in the first DAG")

    @task.python
    def third_task():
        print("Third task in the first DAG")
        os.makedirs(os.path.dirname('/opt/airflow/logs/data'), exist_ok = True)
        with open('/opt/airflow/logs/data/orch_1_file.txt', 'w') as f:
            f.write(f"The child DAG 1 was activated successfully")

    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third

first_dag()
