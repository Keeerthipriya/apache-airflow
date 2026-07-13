from airflow.sdk import dag, task


@dag(
    dag_id="versioned_dag",
    schedule=None,
    catchup=False,
)
def versioned_dag():
    @task 
    def first_task():
        print("First task by Keerthi")

    @task
    def second_task():
        print("This is the second task by Keerthi")

    @task
    def third_task():
        print("This is the third task by Keerthi")


    @task
    #if i add a new task or made any changes to the code it changes the version of the code and we can use this versioning to keep track of the changes made to the code
    def version_task():
        print("This is the version task by Keerthi. DAG version 2.0!")

#Defining taks dependencies
    first = first_task()
    second = second_task()
    third = third_task()
    version= version_task()

    first >> second >> third >> version

#Instantiating the DAG
versioned_dag()