from dag_orchestrate_1 import first_dag
from dag_orchestrate_2 import second_dag
from airflow.sdk import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

@dag
def orchestrate_dags():
    trigger_first_dag = TriggerDagRunOperator(
        task_id = "trigger_first_dag",
        trigger_dag_id = "first_dag",
        wait_for_completion = True
    )
    trigger_second_dag = TriggerDagRunOperator(
        task_id = "trigger_second_dag",
        trigger_dag_id = "second_dag",
        wait_for_completion = True
    )

    trigger_first_dag >> trigger_second_dag
orchestrate_dags()