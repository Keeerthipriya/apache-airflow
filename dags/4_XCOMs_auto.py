from airflow.sdk import dag, task


@dag(dag_id="xcoms_dag_auto", schedule=None,
    catchup=False,)
def xcoms_dag_auto():
    @task.python
    def first_task():
        print("Extracting data... This is the first task")
        fetched_data = {
            "data": [1, 2, 3, 4, 5]
        }
        return fetched_data

    @task.python
    def second_task(data: dict):
        fetched_data = data["data"]
        transformed_data = [item * 2 for item in fetched_data]
        transformed_data_dict = {"transf_data": transformed_data}
        return transformed_data_dict

    @task.python
    def third_task(data: dict):
        load_data = data
        return load_data

    first = first_task()
    second = second_task(first)
    third = third_task(second)

    #no need to define the dependencies as we are passing the output of one task to another task as input so it will automatically create the dependencies between the tasks


xcoms_dag_auto()