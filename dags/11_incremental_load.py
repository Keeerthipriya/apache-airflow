from airflow.sdk import dag,task
from pendulum import datetime
from airflow.timetables.interval import CronDataIntervalTimetable

@dag(
    schedule=CronDataIntervalTimetable("@daily",timezone="America/Halifax"),
    start_date=datetime(year=2026, month=7, day=10, tz="America/Halifax"),
    end_date=datetime(year=2026, month=7, day=15, tz="America/Halifax"),
    catchup=True
)
#running DAG multiple times based on the intervals
#catch up is true it can run the pervious intervals

def incremental_load_dag():

    @task.python
    def incremental_data_fetch(**kwargs):
        date_interval_start = kwargs['data_interval_start']
        date_interval_end = kwargs['data_interval_end']
        print(f"Fetching data from {date_interval_start} to {date_interval_end}")

    @task.bash
    #we use jinja template because we have built in variables
    def incremental_data_process():
        return f"echo 'processing incremental data from {{ data_interval_start }} to {{ data_interval_end }}'"
        #Those variables are Automatically rendered for bash operators no need to fetch manually like in python from jinja template

    fetch_task = incremental_data_fetch()
    process_task = incremental_data_process()
    
    fetch_task >> process_task


incremental_load_dag()