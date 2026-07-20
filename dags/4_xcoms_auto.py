from airflow.sdk import dag, task

@dag
def xcoms_dag():

    @task.python
    def first_task():
        print("Extracting data..This is the first task")
        fetched_data = {"data":[1,2,3,4,5]}
        return fetched_data
    
    @task.python
    def second_task(data_1: dict):
        print("Transform data..This is task two")
        fetched_data = data_1['data']
        transformed_data = {"transformed_data":fetched_data*2}
        return transformed_data

    @task.python
    def third_task(data_2: dict):
        print("Load data..This is the third task")
        load_data = data_2['transformed_data']
        return load_data

    first = first_task()
    second = second_task(first)
    third = third_task(second)

xcoms_dag()
