from datetime import datetime, timedelta
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
import pandas as pd


def get_api_data():
    #url = "https://api.nytimes.com/svc/books/v3/lists.json"
    url = "https://api.nytimes.com/svc/books/v3/lists.json?api-key=06PlzSd5BzV0awSKwsKnnHSCu5mJSEEd&list=combined-print-and-e-book-fiction"
    #headers = {"api-key": "06PlzSd5BzV0awSKwsKnnHSCu5mJSEEd", "list": "combined-print-and-e-book-fiction"}
    headers = {}
    payload = {}  # Add your payload if needed
    print("Starting the API call...")
    response = requests.get(url, headers=headers, data = payload)
    response_json = response.json()
    print("Response: ")
    print(response.text)
    return response_json


def compare_published_date(response_json):
    published_date = response_json["results"][0]["published_date"]
    stored_date = Variable.get("published_date", default_var="")
    print("stored_date: ", stored_date)
    print("published_date: ", published_date)    
    if published_date != stored_date:
        Variable.set("published_date", published_date)
        return "create_table_html_task"
    else:
        return "skip_email_task"


def create_table_html(response_json):
    results = response_json["results"]
    table_data = []
    for result in results:
        rank = result["rank"]
        book_name = result["book_details"][0]["title"]
        table_data.append((rank, book_name))

    df = pd.DataFrame(table_data, columns=["Rank", "Book Name"])
    table_html = df.to_html(index=False)

    return table_html


def send_email(table_html):
    recipients = Variable.get("email_recipients", default_var="").split(",")
    subject = "New Book Rankings"
    body = f"<html><body>{table_html}</body></html>"

    email_operator = EmailOperator(
        task_id="send_email",
        to=recipients,
        subject=subject,
        html_content=body
    )
    email_operator.execute()


default_args = {
    "start_date": datetime(2023, 6, 1),
    "retries": 0,
}

with DAG("book_rankings_dag", default_args=default_args, schedule_interval="0 0 * * *") as dag:
    get_data_task = PythonOperator(
        task_id="get_api_data",
        python_callable=get_api_data
    )

    compare_date_task = BranchPythonOperator(
        task_id="compare_date_task",
        python_callable=compare_published_date,
        op_kwargs={"response_json": "{{ ti.xcom_pull(task_ids='get_api_data') }}"}
    )

    create_table_task = PythonOperator(
        task_id="create_table_html_task",
        python_callable=create_table_html,
        op_kwargs={"response_json": "{{ ti.xcom_pull(task_ids='get_api_data') }}"}
    )

    skip_email_task = DummyOperator(task_id="skip_email_task")

    send_email_task = PythonOperator(
        task_id="send_email",
        python_callable=send_email,
        op_kwargs={"table_html": "{{ ti.xcom_pull(task_ids='create_table_html_task') }}"}
    )

    end_task = DummyOperator(task_id="end_task")

    get_data_task >> compare_date_task
    compare_date_task >> create_table_task >> send_email_task >> end_task
    compare_date_task >> skip_email_task >> end_task
