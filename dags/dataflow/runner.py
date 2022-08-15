import sqlite3
import pandas as pd

def csv_to_sqlite(data_path:str, table_name:str, db_name:str, if_exists="replace", index=False) -> None:
    print(f'Connecting to {db_name}')
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    print(f'Load {data_path} into Pandas DataFrame')
    data = pd.read_csv(data_path)
    print(f'Executing data extraction')
    CREATE_TABLE_QUERY = '''CREATE TABLE IF NOT EXISTS'''
    value_query = f" {table_name} ("
    for cl, dtype in zip(data.columns, data.dtypes):
        cl = cl.replace('-', '_')
        if dtype == 'int64' or dtype == 'float64':
            dtype = 'number'
        else:
            dtype = 'text'
        value_query += f"{cl} {dtype} "
    value_query += ")"
    print(f'Extraction completed. Query created')
    print(f'Running query to {db_name}')
    c.execute(CREATE_TABLE_QUERY + value_query)
    data.to_sql(table_name, conn, if_exists=if_exists, index=index)
    print('Query succesfully executed')
    conn.commit()
    conn.close()
    print('SQLite connection closed')
    
