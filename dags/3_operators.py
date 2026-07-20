from airflow.sdk import dag, task

@dag
def bash_dag():

    @task.python
    def first_task():
        print("First task in the first DAG")
    
    @task.python
    def second_task():
        print("Second task in the first DAG")

    @task.python
    def third_task():
        print("Third task in the first DAG. The DAG is complete now!")

    @task.bash
    def bash_task() -> str:
        print("Bash task in the first DAG")
        return "echo Hi DAG"

    @task.python
    def version_task():
        print("This is the version task. DAG version 2.0")

    first = first_task()
    second = second_task()
    third = third_task()
    bash = bash_task()
    version = version_task()

    first >> second >> third >> bash >> version

bash_dag()
