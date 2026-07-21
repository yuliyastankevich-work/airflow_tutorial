from airflow.sdk import dag, task
from pendulum import datetime

@dag(
        dag_id = "daily_schedule_dag",
        start_date = datetime(year=2026, month=1, day=1, tz="Europe/Warsaw"),
        schedule = "@daily",
        is_paused_upon_creation = False,
        catchup = True
)
def daily_schedule_dag():

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

daily_schedule_dag()
