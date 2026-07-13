from airflow.sdk import dag, task
from pendulum import datetime, duration
from airflow.timetables.trigger import DeltaTriggerTimetable

@dag(
    dag_id="delta_schedule_dag",
    start_date= datetime(year=2026,month=7,day=5,tz="America/Halifax"),
    schedule= DeltaTriggerTimetable(duration(days=3)),#it runs in every 3 days no matter what is the start of the month
    end_date=datetime(year=2026, month=7, day=11, tz="America/Halifax"),
    is_paused_upon_creation=False, #automatically enables DAGs in Airflow UI
    catchup=True  #It will run in all pervious intervals as well  
)
def delta_schedule_dag():

    @task.python
    def first_task():
        print("This is the first task")

    @task.python
    def second_task():
        print("This is the second task")

    @task.python
    def third_task():
        print("This is the third task. DAG complete!")

    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third


delta_schedule_dag()