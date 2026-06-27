from airflow.sdk import dag,task

@dag
def second_job_pipeline():
    @task
    def print_message():
        print("This is the second job pipeline.")

    print_message()


second_job_pipeline()