from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import time
import pandas as pd
import json

def create_table_from_csv_raw(engine, schema_name, table_name: str, df: pd.DataFrame):
    columns = []
    
    # Map pandas dtypes to SQL dtypes
    for column_name, dtype in zip(df.columns, df.dtypes):
        if dtype == 'int64':
            columns.append(f"{column_name} INTEGER")
        elif dtype == 'float64':
            columns.append(f"{column_name} FLOAT")
        else:
            columns.append(f"{column_name} TEXT")
    
    create_table_query = f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} ({', '.join(columns)});"
    
    with engine.connect() as connection:
        connection.execution_options(isolation_level="AUTOCOMMIT")    
        connection.execute(text(create_table_query))
        
        # Prepare the insert query
        placeholders = ', '.join([':' + col for col in df.columns])
        insert_query = f"INSERT INTO {schema_name}.{table_name} ({', '.join(df.columns)}) VALUES ({placeholders});"

        # Insert data
        for index, row in df.iterrows():
            params = {col: row[col] for col in df.columns}  # Create a dictionary from the row
            print(f"Inserting row: {params}")  # Debug output
            connection.execute(text(insert_query), params)  # Pass params as a dictionary



# def upload_dataset(name, df, comments = None):
#     engine = connect_tcp_socket()
#     create_table_from_csv_raw(engine, "datasets",name, df)

#     dataset_id = name # temporary
#     dataset_name  = name
#     created_at  = time.time.now()
#     rows = len(df)
#     insert_dataset_entry(engine, dataset_id, dataset_name, comments, created_at, rows)


def insert_dataset_entry(engine, dataset_id, dataset_name, dataset_comment, created_at, rows, model_ids= {'temp'}, lineage =  '{"temp" : "temp"}', table_path=None, project_id = '001'):
    # Define the SQL insert query
    insert_query = """
    INSERT INTO dataset_hub (dataset_id, dataset_name, dataset_comment, created_at, rows, model_ids, lineage, table_path, project_id)
    VALUES (:dataset_id, :dataset_name, :dataset_comment, :created_at, :rows, :model_ids, :lineage, :table_path, :project_id);
    """
    
    # Prepare parameters for the query
    params = {
        "dataset_id": dataset_id,
        "dataset_name": dataset_name,
        "dataset_comment": dataset_comment,
        "created_at": created_at,
        "rows": rows,
        "model_ids": json.dumps(list(model_ids)),
        "lineage": lineage,
        "table_path": table_path,
        "project_id": project_id
    }
    try:
        with engine.connect() as connection:
            # connection.execution_options(isolation_level="AUTOCOMMIT")    
            print(insert_query, params)
            
            connection.execute(text(insert_query), **params)
            print("Dataset entry inserted successfully.")
    except SQLAlchemyError as e:
        print(f"Error inserting dataset entry: {e}")
