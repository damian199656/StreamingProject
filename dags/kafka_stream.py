from datetime import datetime
# from pendulum import DateTime
# from airflow import DAG
# from airflow.providers.standard.operators.python import PythonOperator
# from airflow.decorators import dag, task
from airflow.sdk import dag, task, chain

default_args = {
    'owner': 'Damian',
    'retries': 3
}

# def get_data():
#     import requests
#     res = requests.get("https://randomuser.me/api/")
#     res = res.json()['results'][0]
#     # print(json.dumps(res, indent=4))
#     return res



@dag(
        start_date=datetime(2025, 6, 7),
        default_args=default_args,
        schedule='@daily',
        catchup=False,
        max_consecutive_failed_dag_runs = 3
)
def stream_dag():
    """
    This DAG streams data from an API and prints the JSON response.
    """

    @task
    def get_data():
        import requests
        res = requests.get("https://randomuser.me/api/")
        res = res.json()['results'][0]
        # print(json.dumps(res, indent=4))
        return res
    
    @task
    def format_data(res):
        import json
        data = {}
        location = res['location']
        data['first_name'] = res['name']['first']
        data['last_name'] = res['name']['last']
        data['email'] = res['email']
        data['gender'] = res['gender']
        data['postcode'] = location['postcode']
        data['username'] = res['login']['username']
        data['dob'] = res['dob']['date']
        data['registered_date'] = res['registered']['date']
        data['phone'] = res['phone']
        data['address'] = f"{location['street']['number']} {location['street']['name']}, {location['city']}, {location['state']}, {location['country']}"
        data['picture'] = res['picture']['medium']
        
        return data


    res = get_data()
    formatted_data = format_data(res)

    
stream_dag();