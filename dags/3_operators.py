from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator


@dag(
    dag_id="operators_dag",
    schedule=None,
    catchup=False,
)
def operators_dag():
    @task
    def first_task():
        print("First task by Keerthi")

    @task
    def second_task():
        print("This is the second task by Keerthi")

    @task.bash
    def bash_task_modern():
        return "echo https://airflow.apache.org/"
    
    bash_task_oldschool=BashOperator(
        task_id="run_bash_oldschool",
        bash_command="echo https://airflow.apache.org/"
    )


#Defining taks dependencies
    first = first_task()
    second = second_task()
    bash_modern= bash_task_modern()
    fourth = bash_task_oldschool

    first >> second >> bash_modern >> fourth

#Instantiating the DAG
operators_dag()