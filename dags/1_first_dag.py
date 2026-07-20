from airflow.sdk import dag, task

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

    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third

first_dag()
