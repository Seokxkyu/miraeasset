from datetime import datetime, timedelta
from textwrap import dedent
import sys
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from airflow.operators.python import (
        PythonOperator,
        PythonVirtualenvOperator,
        BranchPythonOperator,
)

# sys.path.append('/home/kyuseok00/news/py/src/py')  
sys.path.append('/home/kyuseok00/news/airflow/py/src/py')  

from keyword_tfidf import extract_keywords
from extract import crawl_data  
from summarize import summarize_articles
from keyword_naver import keyword_naver
from keywords_bert import extract_keybert

with DAG(
    'news_data',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=5)
    },
    description='DAG for processing news data',
    # schedule=timedelta(days=1),
    schedule_interval='@weekly',
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 9, 9),
    catchup=True,
    tags=['miraeasset', 'news', 'crawling'],
) as dag:
    
    crawl_task = PythonOperator(
        task_id='crawl',
        python_callable=crawl_data,  
        op_kwargs={
            'date': '{{ ds_nodash }}',  
            'output_dir': '/home/kyuseok00/news/data/',
        },
        trigger_rule="all_failed"
    )
    
    summarize_task = PythonOperator(
        task_id='summarize',
        python_callable=summarize_articles,
        op_kwargs={
            'ds_nodash': '{{ ds_nodash }}',
            'output_dir': '/home/kyuseok00/news/data/',
        },
        trigger_rule="all_failed"
    )
    
    keywords_tfidf = PythonOperator(
        task_id='keywords_tfidf',
        python_callable=extract_keywords,  
        op_kwargs={
            'ds_nodash': '{{ ds_nodash }}',  
            'output_dir': '/home/kyuseok00/news/data/',  
            },
    )
    
    keywords_keybert = PythonOperator(
        task_id='keywords_keybert',
        python_callable=extract_keybert,  
        op_kwargs={
            'ds_nodash': '{{ ds_nodash }}',  
            'output_dir': '/home/kyuseok00/news/data/',  
        },
        trigger_rule="all_failed"
    )
    
    keywords_api = PythonOperator(
        task_id='keywords_naver',
        python_callable=keyword_naver,  
        op_kwargs={
            'ds_nodash': '{{ ds_nodash }}',  
            'output_dir': '/home/kyuseok00/news/data/',  
        },
        trigger_rule="all_failed"
    )
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    # start >> crawl_task >> summarize_task >> keywords_tfidf >> keywords_api >> end
    start >> crawl_task >> keywords_keybert >> keywords_tfidf >> summarize_task >> keywords_api >> end
    # start >> keywords_tfidf >> end
