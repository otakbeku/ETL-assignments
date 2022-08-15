from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from dataflow.runner import csv_to_sqlite
import datetime as dt 

default_args = {
    'owner':'Harits',
    'start_date': dt.datetime(2022,8,13),
    'retries':2,
    'retry_delay': dt.timedelta(minutes=1)
}

# df_customer = pd.read_csv("../csv_sample_data/Customer_ID_Superstore.csv")
# df_final = pd.read_csv( "../csv_sample_data/final_superstore.csv")
# df_product = pd.read_csv("../csv_sample_data/Product_ID_Superstore.csv")


with DAG(
    "dag_csv_to_sqlite",
    default_args=default_args,
    schedule_interval='@once'
) as dag:
    load_customer_to_sqlite = PythonOperator(
        task_id = "load_customer_data_into_sqlite",
        python_callable=csv_to_sqlite,
        op_kwargs={
            'data_path': 'csv_sample_data/Customer_ID_Superstore.csv',
            'table_name': 'Customer_superstore',
            'db_name': 'Superstore.db'
        }
        
    )
    
    load_order_to_sqlite = PythonOperator(
        task_id = "load_orders_data_into_sqlite",
        python_callable=csv_to_sqlite,
        op_kwargs={
            'data_path': 'csv_sample_data/final_superstore.csv',
            'table_name': 'Order_superstore',
            'db_name': 'Superstore.db'
        }
        
    )
    
    load_product_to_sqlite = PythonOperator(
        task_id = "load_products_data_into_sqlite",
        python_callable=csv_to_sqlite,
        op_kwargs={
            'data_path': 'csv_sample_data/Product_ID_Superstore.csv',
            'table_name': 'Product_superstore',
            'db_name': 'Superstore.db'
        }
        
    )
    
load_customer_to_sqlite >> load_order_to_sqlite >> load_product_to_sqlite