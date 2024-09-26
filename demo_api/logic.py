import time
import json
import uuid
import random
import time
import json
import uuid
import random
import logging
from io import BytesIO
from datetime import datetime
from typing import List, Dict, Union, Optional
import re
import pandas as pd
from fastapi import FastAPI, HTTPException, File, UploadFile, Form
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware

from sqlalchemy import (
    create_engine, Table, Column, String, Integer, DateTime, JSON, Boolean, MetaData, text, inspect, insert, Engine, select,func
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.postgresql import JSONB

from openai import OpenAI
from logic import *

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
        
def generate_unique_dataset_id(engine):
    with engine.connect() as connection:
        # Query to get the maximum existing dataset_id
        result = connection.execute(text("SELECT MAX(dataset_id) FROM public.dataset_hub WHERE dataset_id ~ '^Data[0-9]+$'"))
        max_id = result.scalar()
        
        if max_id:
            # Extract the number from the max_id and increment it
            current_num = int(max_id[4:])
            next_num = current_num + 1
        else:
            # If no existing IDs, start with 1
            next_num = 1
        
        # Format the new ID
        new_id = f"Data{next_num:02d}"
        
        return new_id

def insert_dataset_hub(engine, dataset_id, name, comment, rows):
    query = """
    INSERT INTO public.dataset_hub (
        dataset_id,
        dataset_name,
        dataset_comment,
        created_at,
        rows,
        model_ids,
        lineage,
        table_path,
        project_id
    )
    VALUES (
        :dataset_id,
        :name,
        :comment,
        CURRENT_TIMESTAMP,
        :rows,
        ARRAY['modelA'],
        '{"parent": null, "version": 1}'::jsonb,
        '/datasets/' || :name,
        'PROJ001'
    )
    """
    
    with engine.begin() as conn:
        conn.execute(text(query), {
            "dataset_id": dataset_id,
            "name": name,
            "comment": comment,
            "rows": rows
        })
    
    logging.info(f"Inserted dataset info into public.dataset_hub for dataset: {dataset_id}")

def insert_into_datasets(engine, schema_name, table_name, df):
    metadata = MetaData()
    table = Table(table_name, metadata, 
        *(Column(c, String) for c in df.columns),
        schema=schema_name
    )
    
    with engine.begin() as conn:
        # Create table if not exists
        table.create(conn, checkfirst=True)
        
        # Insert data
        conn.execute(insert(table), df.to_dict(orient='records'))
    
    logging.info(f"Inserted {len(df)} rows into {schema_name}.{table_name}")

def generate_unique_table_name(engine):
    inspector = inspect(engine)
    existing_tables = inspector.get_table_names(schema='golden_dataset')
    
    index = 1
    while True:
        table_name = f"GoldenData{index:02d}"
        if table_name not in existing_tables:
            return table_name
        index += 1

def create_golden_dataset_table(engine, table_name):
    metadata = MetaData()
    Table(table_name, metadata,
        Column('project_id', String(255)),
        Column('user_id', String(255)),
        Column('call_id', String(255)),
        Column('timestamp', DateTime),
        Column('input', String(255)),
        Column('output', String(255)),
        Column('model', String(255)),
        Column('meta', JSON),
        Column('needs_review', Boolean),
        schema='golden_dataset'
    )
    metadata.create_all(engine)

def insert_golden_dataset_hub(engine, dataset_id, name, comment, created_at, rows, table_path, project_id):
    metadata = MetaData()
    golden_dataset_hub = Table(
        'golden_dataset_hub', metadata,
        Column('golden_dataset_id', String),
        Column('golden_dataset_name', String),
        Column('golden_dataset_comment', String),
        Column('created_at', DateTime),
        Column('rows', Integer),
        Column('lineage', JSON),
        Column('table_path', String),
        Column('project_id', String),
        schema='public'
    )

    lineage = {'source': "Random sample"}  # Simplified lineage

    insert_stmt = insert(golden_dataset_hub).values(
        golden_dataset_id=str(name),
        golden_dataset_name=str("Test"),
        golden_dataset_comment=str(comment) if comment is not None else None,
        created_at=created_at,
        rows=int(rows),
        lineage=lineage,
        table_path=str(table_path),
        project_id=str(project_id)
    )

    with engine.connect() as connection:
        try:
            connection.execute(insert_stmt)
            connection.commit()
            logging.info(f"Successfully inserted data into golden_dataset_hub")
        except Exception as e:
            logging.error(f"Error inserting into golden_dataset_hub: {e}")
            connection.rollback()
            raise

def insert_golden_dataset(engine, df, project_id, table_name):
    metadata = MetaData()
    golden_dataset = Table(
        table_name, metadata,
        Column('project_id', String),
        Column('user_id', String),
        Column('call_id', String),
        Column('timestamp', DateTime),
        Column('input', String),
        Column('output', String),
        Column('model', String),
        Column('meta', JSON),
        Column('needs_review', Boolean),
        schema='golden_dataset'
    )

    with engine.begin() as conn:
        data = []
        for _, row in df.iterrows():
            data.append({
                'project_id': str(project_id),
                'user_id': f"user{random.randint(100, 999)}",
                'call_id': f"CMPL{random.randint(1000000, 9999999)}",
                'timestamp': datetime.now(),
                'input': str(row.get('input', ''))[:255],
                'output': str(row.get('output', ''))[:255],
                'model': "modelA",
                'meta': {'additional_info': "N/A"},
                'needs_review': random.choice([True, False])
            })
        conn.execute(insert(golden_dataset), data)
    
    logging.info(f"Inserted {len(df)} rows into golden_dataset.{table_name}")



def generate_next_model_id(db):
    query = text("SELECT model_id FROM public.model_hub")
    
    with db.connect() as connection:
        result = connection.execute(query).fetchall()
    
    max_number = 0
    for row in result:
        match = re.search(r'model_(\d+)', row[0])
        if match:
            number = int(match.group(1))
            if number > max_number:
                max_number = number
    
    return f"model_{max_number + 1}"

def upsert_model_minimal(db_connection: Engine, model_status: str, dataset_id: str,training_mode: str) -> bool:
    """
    Insert or update minimal model information in the database, using placeholder values for all required fields.
    """
    logger = logging.getLogger(__name__)
    
    # Generate the next model_id
    model_id = generate_next_model_id(db_connection)
    logger.info(f"Upserting minimal model information for model_id: {model_id}")
    
    # Insert query with placeholder values for all fields
    insert_query = text("""
    INSERT INTO public.model_hub 
    (model_id, model_name, model_comment, base_model, training_mode, created_at, eval_metrics, eval_performance,
     training_stats, training_perf, training_logs, model_status, training_params, evaluations, model_path, 
     model_endpoint, project_id, dataset_id)
    VALUES (:model_id, :model_name, :model_comment, :base_model, :training_mode, :created_at, :eval_metrics, 
            :eval_performance, :training_stats, :training_perf, :training_logs, :model_status, :training_params, 
            :evaluations, :model_path, :model_endpoint, :project_id, :dataset_id)
    """)
    
    try:
        with db_connection.connect() as connection:
            # Insert new record with placeholder values
            connection.execute(insert_query, {
                "model_id": model_id,
                "model_name": f"{model_id}",
                "model_comment": "Placeholder comment",
                "base_model": "Placeholder base model",
                "training_mode": f"Model_{training_mode}",
                "created_at": datetime.now().isoformat(),
                "eval_metrics": "{}",
                "eval_performance": "{}",
                "training_stats": "{}",
                "training_perf": "Placeholder performance",
                "training_logs": "Placeholder logs",
                "model_status": model_status,
                "training_params": "{}",
                "evaluations": "{}",
                "model_path": "Placeholder path",
                "model_endpoint": "Placeholder endpoint",
                "project_id": "Placeholder project",
                "dataset_id": dataset_id
            })
            
            connection.commit()
        
        logger.info(f"Minimal model information inserted successfully for model_id: {model_id}")
        return True, model_id
    except SQLAlchemyError as e:
        logger.error(f"Error inserting minimal model information: {str(e)}")
        return False, None


def get_next_golden_version(engine):
    inspector = inspect(engine)
    tables = inspector.get_table_names(schema='golden_dataset')
    golden_versions = [t for t in tables if t.startswith('golden_v')]
    
    if not golden_versions:
        return 'golden_v1'
    
    version_numbers = [int(re.search(r'v(\d+)', t).group(1)) for t in golden_versions]
    next_version = max(version_numbers) + 1
    return f'golden_v{next_version}'

def merge_golden_data_tables(engine, table1, table2, output_table):
    metadata = MetaData()
    
    # Use autoload_with for all tables
    t1 = Table(table1, metadata, autoload_with=engine, schema='golden_dataset')
    t2 = Table(table2, metadata, autoload_with=engine, schema='golden_dataset')
    
    # Create or use existing output table
    if not inspect(engine).has_table(output_table, schema='golden_dataset'):
        output = Table(output_table, metadata, *[c.copy() for c in t1.columns], schema='golden_dataset')
        output.create(engine)
    else:
        output = Table(output_table, metadata, autoload_with=engine, schema='golden_dataset')
    
    rows_merged = 0
    # Merge data from both tables into the output table
    with engine.begin() as conn:
        # Insert data from table1
        ins1 = output.insert().from_select([c.name for c in t1.columns], select(t1))
        result = conn.execute(ins1)
        rows_merged += result.rowcount
        
        # Insert data from table2
        ins2 = output.insert().from_select([c.name for c in t2.columns], select(t2))
        result = conn.execute(ins2)
        rows_merged += result.rowcount
    
    return rows_merged

def count_rows(engine, table_name, schema):
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine, schema=schema)
    
    with engine.connect() as connection:
        result = connection.execute(select(func.count()).select_from(table))
        return result.scalar()

def get_latest_golden_data_tables(engine, n=2):
    inspector = inspect(engine)
    tables = inspector.get_table_names(schema='golden_dataset')
    golden_tables = sorted([t for t in tables if t.startswith('GoldenData')], reverse=True)
    return golden_tables[:n]

def get_latest_golden_version(engine):
    inspector = inspect(engine)
    tables = inspector.get_table_names(schema='golden_dataset')
    golden_versions = [t for t in tables if t.startswith('golden_v')]
    
    if not golden_versions:
        return None
    
    return max(golden_versions, key=lambda x: int(re.search(r'v(\d+)', x).group(1)))
