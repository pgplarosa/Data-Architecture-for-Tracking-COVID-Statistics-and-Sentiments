import datetime
import os
import pandas as pd
import s3fs
import sqlite3


from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.operators.rds import (RdsCreateDbSnapshotOperator , RdsStartExportTaskOperator)
from airflow.providers.amazon.aws.transfers.redshift_to_s3 import RedshiftToS3Operator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from dateutil.relativedelta import relativedelta

from smart_open import smart_open
from sqlalchemy import create_engine


with DAG(
    dag_id='extract_covid_data',
    start_date=datetime.datetime.now()
) as dag:
    
    #### Step 1: Copy from OWID to S3
    copy_step = BashOperator(
        task_id='copy_step',
        bash_command=('curl "https://covid.ourworldindata.org/data/owid-covid-data.csv" '
                      '| aws s3 cp - s3://de2022-final-project/landing/owid-data/owid-covid-data.csv')
    )
    
    
#     #### Step 2: Insert CSV data to RDS
    def insert_to_rds():
        
        # Connect to postgres using pre-defined connection
        conn = PostgresHook(postgres_conn_id='final_project_rds')
        engine = conn.get_sqlalchemy_engine()

        # Source table
        df_full = pd.read_csv('s3://de2022-final-project/landing/owid-data/owid-covid-data.csv')


        # Define columns for each table
        case_cols = ['location', 'date', 'total_cases', 'new_cases', 'new_cases_smoothed',
             'total_cases_per_million', 'new_cases_per_million',
             'new_cases_smoothed_per_million']

        deaths_cols = ['location', 'date', 'total_deaths', 'new_deaths', 'new_deaths_smoothed',
                       'total_deaths_per_million', 'new_deaths_per_million',
                       'new_deaths_smoothed_per_million']

        hospital_cols = ['location', 'date', 'icu_patients', 'icu_patients_per_million', 'hosp_patients',
                         'hosp_patients_per_million', 'weekly_icu_admissions', 'weekly_icu_admissions_per_million',
                         'weekly_hosp_admissions', 'weekly_hosp_admissions_per_million']


        tests_cols = ['location', 'date', 'total_tests', 'new_tests', 'total_tests_per_thousand', 'new_tests_per_thousand',
                      'new_tests_smoothed', 'new_tests_smoothed_per_thousand', 'positive_rate', 'tests_per_case', 'tests_units']

        vaccinations_cols = ['location', 'date', 'total_vaccinations', 'people_vaccinated', 'people_fully_vaccinated',
                             'total_boosters', 'new_vaccinations', 'new_vaccinations_smoothed', 'total_vaccinations_per_hundred',
                             'people_vaccinated_per_hundred', 'people_fully_vaccinated_per_hundred', 'total_boosters_per_hundred',
                             'new_vaccinations_smoothed_per_million', 'new_people_vaccinated_smoothed',
                             'new_people_vaccinated_smoothed_per_hundred']

        other_cols = ['iso_code', 'continent', 'location', 'population', 'population_density', 'median_age',
                      'aged_65_older', 'aged_70_older', 'gdp_per_capita', 'extreme_poverty', 'cardiovasc_death_rate',
                      'diabetes_prevalence', 'female_smokers', 'male_smokers', 'handwashing_facilities',
                      'hospital_beds_per_thousand', 'life_expectancy', 'human_development_index']

        engine.execute(
            """
            DELETE FROM cases where TO_DATE(date, 'YYYY-MM-DD') > (now() - INTERVAL '2 months');
            DELETE FROM deaths where TO_DATE(date, 'YYYY-MM-DD') > (now() - INTERVAL '2 months');
            DELETE FROM hospitals where TO_DATE(date, 'YYYY-MM-DD') > (now() - INTERVAL '2 months');
            DELETE FROM tests where TO_DATE(date, 'YYYY-MM-DD') > (now() - INTERVAL '2 months');
            DELETE FROM vaccinations where TO_DATE(date, 'YYYY-MM-DD') > (now() - INTERVAL '2 months')
            """)
        
        two_mos_ago = datetime.datetime.now() + relativedelta(months=-2)
        recent_entries = df_full.date.astype('datetime64') > (datetime.datetime.now() + relativedelta(months=-2))

        # Slicing the dataframe and inserting to RDS      
        df_full[case_cols][recent_entries].to_sql('cases', con=engine, if_exists='append', index=False)
        df_full[deaths_cols][recent_entries].to_sql('deaths', con=engine, if_exists='append', index=False)
        df_full[hospital_cols][recent_entries].to_sql('hospitals', con=engine, if_exists='append', index=False)
        df_full[tests_cols][recent_entries].to_sql('tests', con=engine, if_exists='append', index=False)
        df_full[vaccinations_cols][recent_entries].to_sql('vaccinations', con=engine, if_exists='append', index=False)

        # Saving CSV copies to S3     
        df_full[case_cols].to_csv('s3://de2022-final-project/gold/oltp/cases.csv', index=False)
        df_full[deaths_cols].to_csv('s3://de2022-final-project/gold/oltp/deaths.csv', index=False)
        df_full[hospital_cols].to_csv('s3://de2022-final-project/gold/oltp/hospitals.csv', index=False)
        df_full[tests_cols].to_csv('s3://de2022-final-project/gold/oltp/tests.csv', index=False)
        df_full[vaccinations_cols].to_csv('s3://de2022-final-project/gold/oltp/vaccinations.csv', index=False)
        df_full[other_cols].drop_duplicates().to_csv('s3://de2022-final-project/gold/oltp/countries.csv', index=False)

    insert_to_rds_step = PythonOperator(
        task_id='insert_to_rds_step',
        python_callable=insert_to_rds
    )
    
    
    #### Step 3:  Insert to Redshift
    
    def insert_to_redshift():
        conn_oltp = PostgresHook(postgres_conn_id='final_project_rds')
        engine_oltp = conn_oltp.get_sqlalchemy_engine()

        conn_olap = RedshiftSQLHook(redshift_conn_id='final_project_redshift')
        engine_olap = conn_olap.get_sqlalchemy_engine()
    
        # Delete most recent 2 months data
        engine_olap.execute("DELETE FROM fact_cases  WHERE TO_DATE(date, 'YYY-MM-DD') > DATEADD('month', -2, GETDATE())")
        engine_olap.execute("DELETE FROM fact_deaths  WHERE TO_DATE(date, 'YYY-MM-DD') > DATEADD('month', -2, GETDATE())")
        engine_olap.execute("DELETE FROM fact_vaccinations  WHERE TO_DATE(date, 'YYY-MM-DD') > DATEADD('month', -2, GETDATE())")
        engine_olap.execute("DELETE FROM fact_hospitals  WHERE TO_DATE(date, 'YYY-MM-DD') > DATEADD('month', -2, GETDATE())")
        engine_olap.execute("DELETE FROM fact_tests  WHERE TO_DATE(date, 'YYY-MM-DD') > DATEADD('month', -2, GETDATE())")

        dim_locations = pd.read_sql('select * from countries', engine_oltp)
        dim_locations.dropna(subset=dim_locations.columns[2:]).to_sql(
            'dim_locations', con=engine_olap, if_exists='append', index=False,
            method='multi', chunksize=1000)

        # Add current date
        cur_date = datetime.datetime.now()
        dim_date = pd.DataFrame()
        dim_date['date'] = [cur_date]
        dim_date['year'] = [cur_date.year]
        dim_date['month'] = [cur_date.month]
        dim_date['day'] = [cur_date.day]
        dim_date.to_sql('dim_dates', con=engine_olap, if_exists='append', index=False,
                        method='multi')

        # Append most recent 2 months data
        fact_cases = pd.read_sql("select * from cases where TO_DATE(date, 'YYYY-MM-DD') > (now() - INTERVAL '2 months')", engine_oltp)
        fact_cases.dropna(subset=fact_cases.columns[2:]).drop(columns=['id']).to_sql(
            'fact_cases', con=engine_olap, if_exists='append', index=False, method='multi',
            chunksize=1000)

        fact_deaths = pd.read_sql("select * from deaths where TO_DATE(date, 'YYYY-MM-DD') > (now() - INTERVAL '2 months')", engine_oltp)
        fact_deaths.dropna(subset=fact_deaths.columns[2:]).drop(columns=['id']).to_sql(
            'fact_deaths', con=engine_olap, if_exists='append', index=False, method='multi',
            chunksize=1000)

        fact_hospitals = pd.read_sql("select * from hospitals where TO_DATE(date, 'YYYY-MM-DD') > (now() - INTERVAL '2 months')", engine_oltp)
        fact_hospitals.dropna(subset=fact_hospitals.columns[2:]).drop(columns=['id']).to_sql(
            'fact_hospitals', con=engine_olap, if_exists='append', index=False, method='multi',
            chunksize=1000)

        fact_tests = pd.read_sql("select * from tests where TO_DATE(date, 'YYYY-MM-DD') > (now() - INTERVAL '2 months')", engine_oltp)
        fact_tests.dropna(subset=fact_tests.columns[2:]).drop(columns=['id']).to_sql(
            'fact_tests', con=engine_olap, if_exists='append', index=False, method='multi',
            chunksize=1000)

        fact_vaccinations = pd.read_sql("select * from vaccinations where TO_DATE(date, 'YYYY-MM-DD') > (now() - INTERVAL '2 months')", engine_oltp)
        fact_vaccinations.dropna(subset=fact_vaccinations.columns[2:]).drop(columns=['id']).to_sql(
            'fact_vaccinations', con=engine_olap, if_exists='append', index=False, method='multi',
            chunksize=1000)
        
    insert_to_redshift_step = PythonOperator(
        task_id='insert_to_redshift_step',
        python_callable=insert_to_redshift
    )
        
    #### Step 4: 
    redshift_locations_to_s3 = RedshiftToS3Operator(
        task_id='redshift_locations_to_s3',
        s3_bucket='de2022-final-project',
        s3_key='gold/olap',
        schema='PUBLIC',
        table='dim_locations',
        redshift_conn_id='final_project_redshift',
        include_header=True,
        table_as_file_name=True,
        unload_options=[
            "DELIMITER AS ','",
            "FORMAT AS CSV",
            "ALLOWOVERWRITE",
            "PARALLEL OFF",
            "HEADER"]
    )
    
    redshift_dates_to_s3 = RedshiftToS3Operator(
        task_id='redshift_dates_to_s3',
        s3_bucket='de2022-final-project',
        s3_key='gold/olap',
        schema='PUBLIC',
        table='dim_dates',
        redshift_conn_id='final_project_redshift',
        include_header=True,
        table_as_file_name=True,
        unload_options=[
            "DELIMITER AS ','",
            "FORMAT AS CSV",
            "ALLOWOVERWRITE",
            "PARALLEL OFF",
            "HEADER"]
    )
    redshift_cases_to_s3 = RedshiftToS3Operator(
        task_id='redshift_cases_to_s3',
        s3_bucket='de2022-final-project',
        s3_key='gold/olap',
        schema='PUBLIC',
        table='fact_cases',
        redshift_conn_id='final_project_redshift',
        include_header=True,
        table_as_file_name=True,
        unload_options=[
            "DELIMITER AS ','",
            "FORMAT AS CSV",
            "ALLOWOVERWRITE",
            "PARALLEL OFF",
            "HEADER"]
    )
    redshift_deaths_to_s3 = RedshiftToS3Operator(
        task_id='redshift_deaths_to_s3',
        s3_bucket='de2022-final-project',
        s3_key='gold/olap',
        schema='PUBLIC',
        table='fact_deaths',
        redshift_conn_id='final_project_redshift',
        include_header=True,
        table_as_file_name=True,
        unload_options=[
            "DELIMITER AS ','",
            "FORMAT AS CSV",
            "ALLOWOVERWRITE",
            "PARALLEL OFF",
            "HEADER"]
    )
    redshift_hospitals_to_s3 = RedshiftToS3Operator(
        task_id='redshift_hospitals_to_s3',
        s3_bucket='de2022-final-project',
        s3_key='gold/olap',
        schema='PUBLIC',
        table='fact_hospitals',
        redshift_conn_id='final_project_redshift',
        include_header=True,
        table_as_file_name=True,
        unload_options=[
            "DELIMITER AS ','",
            "FORMAT AS CSV",
            "ALLOWOVERWRITE",
            "PARALLEL OFF",
            "HEADER"]
    )
    redshift_tests_to_s3 = RedshiftToS3Operator(
        task_id='redshift_tests_to_s3',
        s3_bucket='de2022-final-project',
        s3_key='gold/olap',
        schema='PUBLIC',
        table='fact_tests',
        redshift_conn_id='final_project_redshift',
        include_header=True,
        table_as_file_name=True,
        unload_options=[
            "DELIMITER AS ','",
            "FORMAT AS CSV",
            "ALLOWOVERWRITE",
            "PARALLEL OFF",
            "HEADER"]
    )
    redshift_vaccinations_to_s3 = RedshiftToS3Operator(
        task_id='redshift_vaccinations_to_s3',
        s3_bucket='de2022-final-project',
        s3_key='gold/olap',
        schema='PUBLIC',
        table='fact_vaccinations',
        redshift_conn_id='final_project_redshift',
        include_header=True,
        table_as_file_name=True,
        unload_options=[
            "DELIMITER AS ','",
            "ALLOWOVERWRITE",
            "PARALLEL OFF",
            "HEADER"]
    )
    
    
        
    copy_step >> insert_to_rds_step >> insert_to_redshift_step >> [redshift_locations_to_s3, 
                                                                   redshift_dates_to_s3,
                                                                   redshift_cases_to_s3,
                                                                   redshift_vaccinations_to_s3,
                                                                   redshift_tests_to_s3,
                                                                   redshift_hospitals_to_s3,
                                                                   redshift_deaths_to_s3
                                                                  ]