from airflow.decorators import dag, task
from datetime import datetime

@dag(schedule_interval='@daily', start_date=datetime(2023, 1, 1), catchup=False)
def etl_pipeline():
    
    @task
    def extract():
        # Simulate extracting data from a database
        data = {
            'name': 'Alice',
            'age': 30,
            'city': 'New York'
        }
        return data
    
    @task
    def transform(data):
        # Transform the data
        data['age'] = data['age'] + 1
        return data
    
    @task
    def load(data):
        # Load the data to a data warehouse
        print(f"Loading data: {data}")
    
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)

etl_dag = etl_pipeline()
