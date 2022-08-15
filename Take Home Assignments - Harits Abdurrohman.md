# Take Home Assignments

## ETL Process

ETL is a process within data engineer pipeline to transform data into target source. ETL consists of three operation, known as:

- (E)xtraction: extract data from sources. The main idea of extraction is to get the expected data from many sources. In this phase, a validation test is held to check whether the source has the expected values in a given business domain.
- (T)ransform: transform the extracted data into the correct format of a given target. The Data warehouse collects data from several forms like CSV, JSON, XML, and relational databases. The transform phase consolidates the source format into the target format. Besides changing the format, this phase also carries data cleaning. This process involves needs from a given business and technical context. For example, joining multiple extracted data into single data, aggregating some values, re-ordering the columns, or even encoding columns.
- (L)oad: loads the transformed data to target databases. In this phase, primarily new data is concatenated and rarely overwrites the existing data. The Data warehouse records all transactions.

Documentation of ETL code:

In this assignment, I assume the given data is already cleaned, therefore there is no data cleaning process. The only transformation is to change the data file type from CSV into SQL. My DB engine of choice is SQLite, due lightweight and more favorable in small-scale projects. Eventually, this function expected three mandatory parameters, consists of:

- data_path [*string]*: path of csv file
- table_name [*string*]: name of the given table. Assuming the name of table is different from file name
- db_name [*string*]: path of SQLite db

```Python
# Imports all required module
from airflow.decorators import task

import sqlite3
import pandas as pd

import logging

# Declare logger for ETL
task_logger = logging.getLogger('airflow.task')

# Assign decorator for ETL module
@task.python
def csv_to_sqlite(data_path:str, table_name:str, db_name:str, if_exists="replace", index=False) -> None:
    # This function accepts five parameters
    # data_path [string]: path of csv file
    # table_name [string]: name of the given table
    # db_name [string]: name of SQLite db
    # if_exists [string]: toggle to act if the table already exists. Options: 
    # - 'fail': raise a ValueError
    # - 'replace': drop the existing table then insert new values
    # - 'append': insert new values to the existing table
    
    task_logger.info(f'Connecting to {db_name}')
    # Connector of target SQLite
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    task_logger.info(f'Load {data_path} into Pandas DataFrame')
    # Read data from given path
    data = pd.read_csv(data_path)
    task_logger.info(f'Executing data extraction')
    # Initializing create table query
    CREATE_TABLE_QUERY = '''CREATE TABLE IF NOT EXISTS'''
    value_query = f" {table_name} ("
    # This looping extract the name of the column from the data including the data type
    for cl, dtype in zip(data.columns, data.dtypes):
        cl = cl.replace('-', '_')
        if dtype == 'int64' or dtype == 'float64':
            dtype = 'number'
        else:
            dtype = 'text'
        value_query += f"{cl} {dtype} "
    value_query += ")"
    # End of looping
    task_logger.info(f'Extraction completed. Query created')
    task_logger.info(f'Running query to {db_name}')
    # Executing the create table query
    c.execute(CREATE_TABLE_QUERY + value_query)
    # Insert all the value into the correct column
    data.to_sql(table_name, conn, if_exists=if_exists, index=index)
    task_logger.info('Query succesfully executed')
   	# Commit all the changes
    conn.commit()
    # Closing the connection
    conn.close()
    task_logger.warning('SQLite connection closed')
    

```



