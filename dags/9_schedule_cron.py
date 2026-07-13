from airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.trigger import CronTriggerTimetable

@dag(
    dag_id="schedule_cron_dag",
    start_date= datetime(year=2026,month=7,day=5,tz="America/Halifax"),
    schedule= CronTriggerTimetable("0 16 * * MON-FRI",timezone="America/Halifax"),
    end_date=datetime(year=2026, month=7, day=11, tz="America/Halifax"),
    is_paused_upon_creation=False, #automatically enables DAGs in Airflow UI
    catchup=True  #It will run in all pervious intervals as well  
)
def schedule_cron_dag():

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


schedule_cron_dag()