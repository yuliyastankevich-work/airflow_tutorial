from airflow.sdk import dag, task

@dag
def xcoms_kwargs_dag():

    @task.python
    def first_task(**kwargs):
        ti = kwargs['ti']
        print("Extracting data..This is the first task")
        fetched_data = {"data":[1,2,3,4,5]}
        ti.xcom_push(key='return_result', value = fetched_data)
    
    @task.python
    def second_task(**kwargs):
        ti = kwargs['ti']
        fetched_data = ti.xcom_pull(task_ids = 'first_task', key='return_result')['data']
        print("Transform data..This is task two")
        
        transformed_data = {"transformed_data":fetched_data*2}
        ti.xcom_push(key='return_result', value=transformed_data)

    @task.python
    def third_task(**kwargs):
        ti = kwargs['ti']
        print("Load data..This is the third task")
        load_data = ti.xcom_pull(task_ids = 'second_task', key = 'return_result')['transformed_data']
        return load_data

    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third

  

xcoms_kwargs_dag()
