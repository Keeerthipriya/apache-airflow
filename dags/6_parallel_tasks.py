from airflow.sdk import dag, task


@dag(dag_id="parallel_dag")
def parallel_dag():
    @task.python
    def extract_task(**kwargs):
        print("Extracting Data.....")
        ti = kwargs['ti']
        

        extracted_data_dict = {
            "api_extracted_data": [1, 2, 3],
            "db_extracted_data": [4, 5, 6],
            "s3_extracted_data": [7, 8, 9],
        }
        ti.xcom_push(key="return_value", value=extracted_data_dict)

    @task.python
    def transform_task_api(**kwargs):
        ti = kwargs['ti']
        api_extracted_data = ti.xcom_pull(task_ids="extract_task")['api_extracted_data']
        print(f"Transforming API data: {api_extracted_data}.....")
        

        transformed_api_data = [i * 10 for i in api_extracted_data]
        ti.xcom_push(key="return_value", value=transformed_api_data)

    @task.python
    def transform_task_db(**kwargs):
        ti = kwargs['ti']
        db_extracted_data = ti.xcom_pull(task_ids="extract_task")['db_extracted_data']
        print(f"Transforming DB data: {db_extracted_data}.....")

        transformed_db_data = [i * 100 for i in db_extracted_data]
        ti.xcom_push(key="return_value", value=transformed_db_data)

    @task.python
    def transform_task_s3(**kwargs):
        ti = kwargs['ti']
        s3_extracted_data = ti.xcom_pull(task_ids="extract_task")['s3_extracted_data']
        print(f"Transforming S3 data: {s3_extracted_data}.....")

        transformed_s3_data = [i * 1000 for i in s3_extracted_data]
        ti.xcom_push(key="return_value", value=transformed_s3_data)

    @task.bash
    def load_task(**kwargs):
        print("Loading data.....")
        api_data= kwargs['ti'].xcom_pull(task_ids="transform_task_api")
        db_data= kwargs['ti'].xcom_pull(task_ids="transform_task_db")
        s3_data= kwargs['ti'].xcom_pull(task_ids="transform_task_s3")
        return "echo 'Loaded Data: {api_data}, {db_data}, {s3_data}'"
    
    #Most difficult part is task dependencies in parallel tasks, we can use list to define the dependencies between the tasks
    extract = extract_task()
    api_transform = transform_task_api()
    db_transform = transform_task_db()
    s3_transform = transform_task_s3()
    load = load_task()
    extract >> [api_transform, db_transform, s3_transform] >> load

#Instantiating the DAG
parallel_dag()