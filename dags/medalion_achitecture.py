from datetime import datetime
from airflow.sdk import dag,task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

@dag(
    dag_id="chess_medalion_architecture",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=['spark', 'chess', 'bronze_layer']
)
def spark_job_pipeline():

    run_spark_job = SparkSubmitOperator(
        task_id='chess_bronze_layer_job',
        conn_id='spark_default',        # Airflow connection ID
        application='/opt/airflow/jobs/bronze_layer.py',
        name='chess_bronze_layer_job',
        verbose=True,
        deploy_mode='client',
        env_vars={
            "PYSPARK_PYTHON": "python3",         # Tells the Spark workers to use their default python3 (which is 3.10)
            "PYSPARK_DRIVER_PYTHON": "python3",  # Tells Airflow/Driver to use its default python3 (which is 3.13)
        },
        packages="org.postgresql:postgresql:42.7.3"
        
    )

    @task(task_id="spark_message")
    def spark_message():
        print("Spark job has been submitted successfully.") 

    @task.bash(task_id="dbt_run_databricks")
    def dbt_run() -> str:
        return """
            cd /opt/airflow/dbt_daabricks && \
            dbt run --select bronze.incq --profiles-dir /opt/airflow/dbt_daabricks\
            --no-partial-parse
            """
    @task(task_id="dbt_message")
    def dbt_message():
        print("dbt job has been submitted successfully.") 

    run_spark_job >> spark_message()>>dbt_run()>>dbt_message()

spark_job_pipeline()