from airflow.sdk import dag, task

@dag
def versioned_dag():

    @task.python
    def first_task():
        print("First task in the first DAG")
    
    @task.python
    def second_task():
        print("Second task in the first DAG")

    @task.python
    def third_task():
        print("Third task in the first DAG. The DAG is complete now!")

    @task.python
    def forth_task():
        print("Forth task in the first DAG. The DAG is complete now!")

    @task.python
    def version_task():
        print("This is the version task. DAG version 2.0")

    first = first_task()
    second = second_task()
    third = third_task()
    forth = forth_task()
    version = version_task()

    first >> second >> third >> forth >> version

versioned_dag()
