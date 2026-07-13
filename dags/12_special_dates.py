from airflow.sdk import dag,task
from pendulum import datetime
from airflow.timetables.events import EventsTimetable

special_dates=EventsTimetable(event_dates=[
    datetime(2026,7,1),
    datetime(2026,7,5),
    datetime(2026,7,10),
    datetime(2026,7,16)
])

@dag(
    schedule=special_dates,
    start_date=datetime(year=2026, month=7, day=1, tz="America/Halifax"),
    end_date=datetime(year=2026, month=7, day=15, tz="America/Halifax"),
    catchup=True
)
#running DAG multiple times based on the intervals
#catch up is true it can run the pervious intervals

def special_dates_dag():

    @task.python
    def special_event_task(**kwargs):
        execution_date=kwargs['data_interval_start']
        print(f"Running task for special event on {execution_date}")

    special_event = special_event_task()

special_dates_dag()