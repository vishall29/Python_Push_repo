import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Define the file paths
input_file_path = '/path/to/your/input_file.csv'  # Update this with the correct file path
output_file_path = '/path/to/your/output_file.csv'  # Update this with the correct file path

# Step 2: Define default arguments
default_args = {
    'owner': 'user',
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Step 3: Define the DAG
with DAG(
        'etl_local_csv_pipeline',
        default_args=default_args,
        schedule_interval='30 8 * * *',  # 1:30 PM IST
        catchup=False
) as dag:
    # Step 4: Define task functions
    def extract(**kwargs):
        # Read the CSV file from the local file system
        df = pd.read_csv(input_file_path)
        return df.to_dict()  # Return the DataFrame as a dictionary so it can be passed between tasks


    def transform(**kwargs):
        # Fetch the DataFrame from the previous task (Extract)
        ti = kwargs['ti']  # Task Instance to fetch data between tasks
        df_dict = ti.xcom_pull(task_ids='extract_task')
        df = pd.DataFrame(df_dict)

        # Add a new column (for example, adding a static value column)
        df['new_column'] = 'Transformed Value'
        return df.to_dict()  # Return the transformed DataFrame as a dictionary


    def load(**kwargs):
        # Fetch the transformed DataFrame from the previous task (Transform)
        ti = kwargs['ti']
        transformed_df_dict = ti.xcom_pull(task_ids='transform_task')
        transformed_df = pd.DataFrame(transformed_df_dict)

        # Save the transformed DataFrame back to a CSV file
        transformed_df.to_csv(output_file_path, index=False)


    # Step 5: Define PythonOperators
    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform,
        provide_context=True,  # This allows access to `kwargs`
    )

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load,
        provide_context=True,
    )

    # Step 6: Set task dependencies
    extract_task >> transform_task >> load_task
