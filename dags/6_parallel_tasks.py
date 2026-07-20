from airflow.sdk import dag, task

@dag
def parallel_dag():

    @task.python
    def extraction(**kwargs):
        ti = kwargs['ti']
        print("Extracting data..This is the first task")
        extracted_data = {"api_extr_data": [1,2,3],
                          "db_extr_data":[4,5,6],
                          "s3_extr_data":[7,8,9]}
        ti.xcom_push(key = "return_value", value = extracted_data)
    
        

    
    @task.python
    def transform_api(**kwargs):
        ti = kwargs['ti']
        fetched_data = ti.xcom_pull(task_ids = 'extraction')['api_extr_data']
        print("Transform API extracted data..This is task two")
        
        transformed_data = [i*10 for i in fetched_data]
        ti.xcom_push(key='return_value', value=transformed_data)

    @task.python
    def transform_db(**kwargs):
        ti = kwargs['ti']
        print("Transform DB extracted data..This is task three")
        extracted_data = ti.xcom_pull(task_ids = 'extraction')['db_extr_data']
        transformed_data = [i*100 for i in extracted_data]
        ti.xcom_push(key='return_value', value=transformed_data)

    @task.python
    def transform_s3(**kwargs):
        ti = kwargs['ti']
        print("Transform S3 extracted data..This is task four")
        extracted_data = ti.xcom_pull(task_ids = 'extraction')['s3_extr_data']
        transformed_data = [i*1000 for i in extracted_data]
        ti.xcom_push(key='return_value', value=transformed_data)

    @task.bash
    def load_task(**kwargs):
        ti = kwargs['ti']
        api_data = ti.xcom_pull(task_ids = 'transform_api')
        db_data = ti.xcom_pull(task_ids = 'transform_db')
        s3_data = ti.xcom_pull(task_ids = 'transform_s3')

        return f"echo 'Loaded data: {api_data}, {db_data}, {s3_data}'"


    extract = extraction()
    api = transform_api()
    db = transform_db()
    s3 = transform_s3()
    load = load_task()

    extract >> [api, db, s3] >> load

  

parallel_dag()
