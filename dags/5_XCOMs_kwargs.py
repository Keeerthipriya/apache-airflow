from airflow.sdk import dag, task


@dag(dag_id="xcoms_dag_kwargs")
def xcoms_dag_kwargs():
    @task.python
    def first_task(**kwargs):
        ti = kwargs["ti"]#task instance object is used to push and pull data to and from xcoms
        print("Extracting data... This is the first task")
        fetched_data = {
            "data": [1, 2, 3, 4, 5]
        }
        ti.xcom_push(key="return_result", value=fetched_data)

    @task.python
    def second_task(**kwargs):
        ti = kwargs["ti"]
        fetched_data = ti.xcom_pull(task_ids="first_task", key="return_result")
        print("Transforming data... This is second task")
        transformed_data = [item * 2 for item in fetched_data["data"]]
        transformed_data_dict = {"transf_data": transformed_data}
        ti.xcom_push(key="return_result", value=transformed_data_dict)

    @task.python
    def third_task(**kwargs):
        ti = kwargs["ti"]
        load_data = ti.xcom_pull(task_ids="second_task", key="return_result")
        return load_data

    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third


xcoms_dag_kwargs()