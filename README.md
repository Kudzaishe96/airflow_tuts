# DATA ENGINEERING WITH DOCKER, AIRFLOW ,PYSPARK AND DBT
## ETL project using Apache Airflow,Docker,Pyspark and DBT


## Table Of Contents

- [ Project Overview ](#Project-Overview)
- [ Data Source ](#Data-Source)
- [ Tools ](#Tools)
- [ Docker ](#Docker)
- [ Data Extraction and Cleaning  ](#Data-Extraction-(Bronze-Layer))


### Project Overview
Ilustrates the extraction of data using python with REST APIs while implementing the Medallion Architecture for Data Warehousing.

### Data Source
API :Chess.com

### Tools
- API
- Docker
- Apache Airflow
- Apache Spark
- DBT
- Postgress

### Installing Dependancies and build a Docker Custome Container
- Create two docker files , 1 for airflow and other for custome building image for spark and install DBT python and other depandacies you wish to run inside Airflow
- Use docker compose build -d and docker compose up for initalising custome builds and run the container in docker


### Data Extraction and Cleaning
1. Call out various Api cals from chess.com
2. Convert the data to pandas to spark dataframes
4. Extract it to the Postgress SQL Database

### Docker
1. Compose a custome docker image which countains spark, airflow,postgress and Dbt
2. Use port 8080 to have access to arflow and configure the spark connection (spark://spark-master:7077)
3. Use Aiflow(localhost://8080) to trigger a job with both pyspark and dbt run commands
   

 #  The End
