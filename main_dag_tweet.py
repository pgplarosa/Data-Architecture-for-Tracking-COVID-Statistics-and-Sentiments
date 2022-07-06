import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from tweet_data import * 

with DAG(
    dag_id='covid_tweet',
    start_date=datetime(2022, 5, 27)
) as dag:
    
    scrape_tweets = PythonOperator(
        task_id='scrape_tweets',
        python_callable=scrape_tweets
    )

    
    append_to_dynamo = PythonOperator(
        task_id='append_to_dynamo',
        python_callable=insert_to_dynamo
    )
    
    set_dynamo_to_s3 = PythonOperator(
        task_id='set_dynamo_to_s3',
        python_callable=dynamo_to_s3
    )
    
    
    scrape_tweets >> append_to_dynamo >> set_dynamo_to_s3
