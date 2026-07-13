from airflow.sdk import dag, task


@dag(
    dag_id="first_dag",
    schedule=None,
    catchup=False,
)
def first_dag():
    @task
    #we can also use @task.python or @task.bash according to the type of the if we keep @task it will take python as default
    def first_task():
        print("First task by Keerthi")

    @task
    def second_task():
        print("This is the second task by Keerthi")

    @task
    def third_task():
        print("This is the third task by Keerthi")

#Defining taks dependencies
    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third

#Instantiating the DAG
first_dag()
