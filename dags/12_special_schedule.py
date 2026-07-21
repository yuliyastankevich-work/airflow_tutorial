from airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.events import EventsTimetable

special_dates = EventsTimetable(event_dates = [
    datetime(2026,1,1),
    datetime(2026,1,15),
    datetime(2026,1,27),
    datetime(2026,1,30)
])

@dag(
        schedule = special_dates,
        start_date = datetime(year=2026, month=1, day = 1, tz= 'Europe/Warsaw'),
        end_date = datetime(year=2026, month=1, day = 31, tz = 'Europe/Warsaw'),
        catchup = True,
        is_paused_upon_creation = False
)

def special_dates_dag():

    @task.python
    def special_event_task(**kwargs):
        execution_date = kwargs['logical_date']
        print(f"Runnin task for special event on {execution_date}")
    
    special_event = special_event_task()

special_dates_dag()