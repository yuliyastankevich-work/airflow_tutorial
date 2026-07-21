from airflow.sdk import dag, task
from pendulum import datetime, duration
from airflow.timetables.trigger import DeltaTriggerTimetable

@dag(
        dag_id = "daily_schedule_delta_dag",
        start_date = datetime(year=2026, month=7, day=14, tz="Europe/Warsaw"),
        schedule = DeltaTriggerTimetable(duration(days=2)),
        end_date = datetime(year=2026, month=7, day=22, tz="Europe/Warsaw"),
        is_paused_upon_creation = False,
        catchup = True
)
def daily_schedule_delta_dag():

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

daily_schedule_delta_dag()
