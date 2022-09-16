from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pymongo import MongoClient
import pandas as pd
import json

default_args = {
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'owner': 'admin',
    'retries': 1,
    'retries_delay': timedelta(minutes=1)
}

dag = DAG('task5', default_args=default_args, schedule_interval='@once')


def exp_transform():
    data = pd.read_csv("./tiktok_google_play_reviews.csv")
    df = data.dropna(axis=0, how="any", thresh=None, subset=None, inplace=False)
    df.fillna("-", inplace=True)
    df['at'] = pd.to_datetime(df['at'])
    df = df.sort_values(by='at')
    df['content'].replace(r"[^\s\w!?'\\-]", "", regex=True, inplace=True)
    df.to_csv('transformed_data.csv')


def upload():
    df = pd.read_csv('transformed_data.csv')
    client = MongoClient(host='mongodb://localhost:27017')
    data = df.to_dict(orient='records')
    db = client['task5']
    db.Iris.insert_many(data)


exp_transform_operator = PythonOperator(task_id='exp', python_callable=exp_transform, dag=dag)
upload_operator = PythonOperator(task_id='load', python_callable=upload, dag=dag)

exp_transform_operator >> upload_operator
