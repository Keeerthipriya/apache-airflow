from airflow.sdk import dag, task
import os


@dag(
    dag_id="first_orchestrator_dag",
)
def first_orchestrator_dag():

    @task.python
    def first_task():
        print("This is the first task")

    @task.python
    def second_task():
        print("This is the second task")

    @task.python
    def third_task():
    #Ensure the directory exists
        os.makedirs(os.path.dirnme("/opt/airflow/logs/data"), exist_ok=True)
   
        # Simulate data processing by writing to a file
        with open("/opt/airflow/logs/dataoutput_1.txt", 'w') as f:
            f.write(f"Data processed successfully")
        
    # Creating task instances
    first = first_task()
    second = second_task()
    third = third_task()

    # Defining task dependencies
    first >> second >> third


# Instantiating the DAG
first_orchestrator_dag()