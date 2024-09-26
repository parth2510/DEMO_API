import requests
from fastapi import FastAPI, HTTPException, File, UploadFile, Form
from pydantic import BaseModel
from typing import List,Optional
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import os
import asyncio
import subprocess
from openai import OpenAI
from fastapi import HTTPException
import sqlalchemy
from openai import OpenAI
from io import BytesIO
from logic  import *
from datetime import datetime
from typing import Optional
import pandas as pd
from io import BytesIO
from datetime import datetime
import uuid
from sqlalchemy import  text
import logging

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins. Replace with specific origins if needed.
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods. Replace with specific methods if needed.
    allow_headers=["*"],  # Allows all headers. Replace with specific headers if needed.
)



# Model classes for the input and output of APIs
class AddToGoldenRequest(BaseModel):
    table_name: Optional[str] = None

class Config(BaseModel):
    project_name: str
    run_name: str
    push_to: str
    model_name: str
    save_model_name: str
    max_seq_length: int
    load_in_4bit: bool
    per_device_train_batch_size: int
    gradient_accumulation_steps: int
    warmup_steps: int
    num_train_epochs: int
    learning_rate: float
    weight_decay: float
    seed: int
    output_dir: str
    save_steps: int


class MarkDatasetRequest(BaseModel):
    dataset_id: str
    dataset_path: str
    type: str  # new or merged

class CreateModelRequest(BaseModel):
    training_mode: str  # auto or manual
    dataset: str
    eval_metric: str
    config_file: UploadFile

class DeleteDeployArchiveModelRequest(BaseModel):
    model_id: str
    meta: Optional[str] = None  # JSON string

class ModelResponse(BaseModel):
    model_name: str
    base_model: str
    dataset: str
    eval_metrics: str
    performance: float
    status: str

class Message(BaseModel):
    role: str
    content: str

class ModelInferenceRequest(BaseModel):
    model: str
    messages: List[Message]
    stream: bool
    meta: Optional[str] = None

class AddArenaRequest(BaseModel):
    model1: str
    model2: str

class AddGoldenRequest(BaseModel):
    model: str
    ref_model_1: str
    ref_model_2: str
    ref_model_3: str
    metric_1: str
    metric_2: str
    metric_3: str

class ModelInferenceResponse(BaseModel):
    output: str
    status: str

class AddArenaResponse(BaseModel):
    status: str
    message: str

class AddGoldenResponse(BaseModel):
    status: str
    message: str


# Request models
class MergeGoldenRequest(BaseModel):
    call_ids: List[str]
    source_dataset_id: Optional[str] = None
    name: str
    comments: Optional[str] = None  # Made optional

class MergeDatasetRequest(BaseModel):
    call_ids: List[str]
    source_dataset_id: Optional[str] = None
    destination_dataset_id: str
    name: str
    comments: Optional[str] = None  # Made optional

class CreateNewDatasetRequest(BaseModel):
    call_ids: List[str]
    source_dataset_id: Optional[str] = None
    name: str
    comments: Optional[str] = None  # Made optional

class UpdateGoldenRequest(BaseModel):
    omitted_call_ids: List[str]
    comments: Optional[str] = None  # Made optional

# Dummy Response models
class MergeGoldenResponse(BaseModel):
    previous_rows: int
    new_rows: int

class MergeDatasetResponse(BaseModel):
    new_rows: int
    golden_rows: int

class CreateNewDatasetResponse(BaseModel):
    new_rows: int
    golden_rows: int

class UpdateGoldenResponse(BaseModel):
    previous_rows: int
    new_rows: int

def connect_tcp_socket() -> sqlalchemy.engine.base.Engine:
    """Initializes a TCP connection pool for a Cloud SQL instance of Postgres."""

    pool = sqlalchemy.create_engine(
        # Equivalent URL:
        # url = 'genloopv1:us-central1:genloop-postgresv1'
        # url = 'postgresql+psycopg2://postgres:genloop@123@34.68.9.168:5302/postgresv1'
        sqlalchemy.engine.url.URL.create(
            drivername="postgresql+psycopg2",
            username = "postgres",
            password='genloop@123', #TODO pick from env
            host='34.68.9.168', #TODO pick from env
            port='5432',
            database='client1',
        )
    )
    return pool

# Dummy in-memory storage
models_db = {}
datasets_db = {}

@app.post("/mark_dataset")
async def mark_dataset(request: MarkDatasetRequest):
    return {
        "is_success": True,
        "golden_data_entries": 100,  # Dummy data
        "dataset_entries": 95  # Dummy data
    }
logging.basicConfig(level=logging.DEBUG)


@app.post("/upload_dataset")
async def upload_dataset_response(
    name: str = Form(...), 
    comment: str = Form(None),
    file: UploadFile = File(...)):
    if file:
        content = await file.read()
        print(f"File received with content length: {len(content)}")
        # Use BytesIO to read the contents as a CSV
        df = pd.read_csv(BytesIO(content))
        
        engine = connect_tcp_socket()

        # Create a new DataFrame with 10% random samples
        sample_size = int(len(df) * 0.1)
        golden_samples = df.sample(n=sample_size, random_state=42)
        
        # Remove these samples from the original DataFrame
        main_df = df.drop(golden_samples.index)

        # model_id = generate_next_model_id(db_connection)
        
        # Insert the 90% data into the datasets schema
        insert_into_datasets(engine, "datasets", name, main_df)

        # Generate data for golden_dataset_hub
        golden_dataset_id = str(uuid.uuid4())
        golden_dataset_name = generate_unique_table_name(engine)
        created_at = datetime.now()
        rows = len(golden_samples)
        lineage_table_path = f"/golden_datasets/{golden_dataset_name}"
        project_id = "PROJ001"  # Generic project ID

        # Insert into golden_dataset_hub
        insert_golden_dataset_hub(engine, golden_dataset_id, golden_dataset_name, comment, created_at, rows, lineage_table_path, project_id)

        # Create and insert samples into golden_dataset
        create_golden_dataset_table(engine, golden_dataset_name)
        insert_golden_dataset(engine, golden_samples, project_id, golden_dataset_name)
        # Generate unique dataset_id for dataset_hub
        dataset_id = generate_unique_dataset_id(engine)
        # Insert into public.dataset_hub
        insert_dataset_hub(engine, dataset_id, name, comment, len(main_df))

    return {
        "status": "success",
        "message": f"Data uploaded. Main dataset: {len(main_df)} rows, Golden dataset: {len(golden_samples)} rows. Dataset ID: {dataset_id}"
    }


def quote_identifier(identifier):
    """
    Quote an identifier to be used in SQL queries.
    This preserves the case of the identifier.
    """
    return f'"{identifier}"'
    
@app.post("/create_model")
async def create_model(
    training_mode: str = Form(...),
    dataset: str = Form(...),
    eval_metric: str = Form(...),
    data_file: Optional[UploadFile] = File(None)
):
    logger = logging.getLogger(__name__)
    if training_mode == "manual" and data_file is None:
        raise HTTPException(status_code=400, detail="data_file is required for manual training mode")
    
    if training_mode == "auto" and data_file is not None:
        logger.warning("data_file is not required for auto training mode and will be ignored")
    
    # Set initial model status
    model_status = "traning"
    
    # Get database connection
    engine = connect_tcp_socket()
    
    # Call the upsert function
    success, model_id = upsert_model_minimal(engine, model_status, dataset,training_mode)
    
    if success:
        message = f"Model training for {dataset} has been queued and will start shortly. Model ID: {model_id}"
    else:
        raise HTTPException(status_code=500, detail="Failed to create model entry in database.")
    
    return {"message": message, "model_id": model_id}

    # if not data_file:
    #     raise HTTPException(status_code=400, detail="Config file is required")

    # # Read and parse the uploaded config file
    # try:
    #     config_content = await data_file.read()
    #     config_data = json.loads(config_content)
    # except json.JSONDecodeError:
    #     raise HTTPException(status_code=400, detail="Invalid JSON in config file")

    # # Validate the config data
    # try:
    #     if 'config' not in config_data:
    #         raise ValueError("Missing 'config' key in the JSON file")
    #     config_dict = config_data['config']
    #     config_dict.pop('eval_metric', None)
    #     config = Config(**config_dict)
    # except ValueError as e:
    #     raise HTTPException(status_code=400, detail=f"Invalid config data: {str(e)}")

    # # Quote the dataset name
    # quoted_dataset = quote_identifier(dataset)

    # # Prepare the payload for the request
    # quoted_dataset = quoted_dataset.strip('"')
    
    # payload = json.dumps({
    #     "dataset_id": quoted_dataset,
    #     "config": {
    #         **config.dict(),
    #         "eval_metric": eval_metric,
    #         "training_mode": training_mode
    #     }
    # })

    # # Make the request to the training endpoint
    # url = "http://34.70.245.171:5000/train"
    # headers = {
    #     'Content-Type': 'application/json'
    # }

    # try:
    #     response = requests.post(url, headers=headers, data=payload)
    #     response.raise_for_status()
    # except requests.RequestException as e:
    #     raise HTTPException(status_code=500, detail=f"Error making request to training endpoint: {str(e)}")

    # return response.json()

@app.post("/delete_model")
async def delete_model(request: DeleteDeployArchiveModelRequest):
    model_id = request.model_id
    
    return {"HTTP Status code": 200}
    
@app.post("/deploy_model")
async def deploy_model(request: DeleteDeployArchiveModelRequest):
    
    return {"HTTP Status code": 200}

@app.post("/test_model")
async def test_model(request: DeleteDeployArchiveModelRequest):
    
    return {"HTTP Status code": 200}
    

@app.post("/archive_model")
async def archive_model(request: DeleteDeployArchiveModelRequest):
    model_id = request.model_id
    
    return {"HTTP Status code": 200}
    

@app.post("/model_inference", response_model=ModelInferenceResponse)
async def model_inference(request: ModelInferenceRequest):
    # Dummy implementation

    try:
        input = request.messages[-1].content
        openai_api_key = "LOAD FROM ENV"

        client = OpenAI(
            api_key=openai_api_key
        )

        response = client.chat.completions.create(
        model = "gpt-4o-mini",
      messages = [ 
            {
        "role": "system",
        "content": "Generate a suitable headline for given input of a financial news article. Only output the headline not the instruction and input texts."
            },
            {
            "role": "user",
            "content": input}],
        max_tokens = 85,
        temperature = 0.7,
        stream = False
        )

        print(response.choices[0].message.content)
        return {
        "output": response.choices[0].message.content,
        "status": "success"
    }
    except Exception as e:
        print(e)
        return {
            "output": "This is a dummy output",
            "status": "success"}

# @app.post("/model_inference", response_model=ModelInferenceResponse)
# async def model_inference(request: ModelInferenceRequest):
#     # Dummy implementation

#     try:
#         input = request.messages[-1].content
#         openai_api_key = "BW8J_qlpGFAD51VXihUoNAHxsyN1jXkvQLsMf4nueHI"
#         openai_api_base = "http://34.122.194.201:8000/v1"

#         client = OpenAI(
#             base_url=openai_api_base,
#             api_key=openai_api_key,
#         )

#         response = client.chat.completions.create(
#         model = "genloop/fin-news-headline-gen_mistral-nemo-instruct-merged",
#         messages = [ 
#             {
#         "role": "system",
#         "content": "Generate a suitable headline for given input of a financial news article. Only output the headline not the instruction and input texts."
#             },
#             {
#             "role": "user",
#             "content": input
#             }
#         ],
#         max_tokens = 85,
#         temperature = 0.7,
#         stream = False
#         )

#         print(response.choices[0].message.content)
#         return {
#         "output": response.choices[0].message.content,
#         "status": "success"
#     }
#     except Exception as e:
#         print(e)
#         return {
#             "output": "Something went wrong, please try after some time",
#             "status": "success"
#         }

@app.post("/add_arena", response_model=AddArenaResponse)
async def add_arena(
    model1: str = Form(...), 
    model2: str = Form(...),
    file: UploadFile = File(...)
):
    
    if file:
        content = await file.read()
        print(f"File received with content length: {len(content)}")

    return {
        "status": "success",
        "message": f"Arena created with models '{model1}' and '{model2}', and file uploaded."
    }
    
@app.post("/add_to_golden")
async def add_to_golden(request: AddToGoldenRequest):
    engine = connect_tcp_socket()
    inspector = inspect(engine)
    
    if request.table_name:
        current_table = request.table_name
        latest_tables = get_latest_golden_data_tables(engine, 1)
        if latest_tables:
            previous_table = latest_tables[0]
        else:
            raise HTTPException(status_code=400, detail="No existing GoldenData tables found")
    else:
        latest_tables = get_latest_golden_data_tables(engine)
        if len(latest_tables) < 2:
            raise HTTPException(status_code=400, detail="Not enough GoldenData tables to merge")
        current_table, previous_table = latest_tables[0], latest_tables[1]
    
    new_table_name = get_next_golden_version(engine)
    previous_golden = get_latest_golden_version(engine)
    
    if previous_golden:
        previous_rows = count_rows(engine, previous_golden, 'golden_dataset')
    else:
        previous_rows = 0
    
    rows_merged = merge_golden_data_tables(engine, previous_table, current_table, new_table_name)
    new_rows = count_rows(engine, new_table_name, 'golden_dataset')
    
    message = f"Created {new_table_name} by merging {previous_table} and {current_table}"
    
    new_golden_id = generate_unique_table_name(engine)
    insert_golden_dataset_hub(
        engine,
        new_golden_id,
        new_table_name,
        f"Merged from {previous_table} and {current_table}. Previous rows: {previous_rows}, New rows: {new_rows}, Rows merged: {rows_merged}",
        datetime.now(),
        new_rows,
        f"/golden_datasets/{new_table_name}",
        "PROJ001"
    )
    
    logging.info(message)
    return {
        "status": "success", 
        "message": message,
        "rows_merged": rows_merged
    }

# Endpoint for merge_dataset
@app.post("/merge_dataset", response_model=MergeDatasetResponse)
async def merge_dataset(request: MergeDatasetRequest):
    engine = connect_tcp_socket()
    with engine.connect() as connection:
        connection.execution_options(isolation_level="AUTOCOMMIT")
        query = text("""
            INSERT INTO dataset_hub (dataset_id, dataset_name, created_at, rows, model_ids, project_id, dataset_comment)
            VALUES (:dataset_id, :dataset_name, :created_at, :rows, :model_ids, :project_id, :dataset_comment);
            """)
            
        connection.execute(query, {
                'dataset_id': request.name ,
                'dataset_name': request.name,
                'created_at': '2024-09-04T00:00:00Z',  # Correctly quoted timestamp
                'rows': int(10*len(request.call_ids)*0.9),
                'model_ids': '{}',  # Properly formatted array
                'project_id': 'PROJECT01',
                'dataset_comment': 'beta testing'
        })
        #golden dataset
        query = text("""
            INSERT INTO golden_dataset_hub (golden_dataset_id, golden_dataset_name, created_at, rows, table_path, project_id, golden_dataset_comment, lineage)
            VALUES (:golden_dataset_id, :golden_dataset_name, :created_at, :rows, :table_path, :project_id, :golden_dataset_comment, :lineage);
            """)
            
        connection.execute(query, {
                'golden_dataset_id': "GoldenData" + request.name ,
                'golden_dataset_name': request.name,
                'created_at': '2024-09-04T00:00:00Z',  # Correctly quoted timestamp
                'rows': int(10*len(request.call_ids)*0.1),
                'table_path': '/golden_datasets/datasets',  # Properly formatted array
                'project_id': 'PROJECT01',
                'golden_dataset_comment': 'beta testing',
                'lineage': '{"source": "Merged", "parents": ["GoldenData01", "GoldenData02"]}'
        })
    return MergeDatasetResponse(
        new_rows=int(10*len(request.call_ids)*0.9),
        golden_rows=int(10*len(request.call_ids)*0.1)
    )

# Endpoint for create_new_dataset
@app.post("/create_new_dataset", response_model=CreateNewDatasetResponse)
async def create_new_dataset(request: CreateNewDatasetRequest):

    engine = connect_tcp_socket()
    with engine.connect() as connection:
        connection.execution_options(isolation_level="AUTOCOMMIT")
        query = text("""
            INSERT INTO dataset_hub (dataset_id, dataset_name, created_at, rows, model_ids, project_id, dataset_comment)
            VALUES (:dataset_id, :dataset_name, :created_at, :rows, :model_ids, :project_id, :dataset_comment);
            """)
            
        connection.execute(query, {
                'dataset_id': request.name ,
                'dataset_name': request.name,
                'created_at': '2024-09-04T00:00:00Z',  # Correctly quoted timestamp
                'rows': int(10*len(request.call_ids)*0.9),
                'model_ids': '{}',  # Properly formatted array
                'project_id': 'PROJECT01',
                'dataset_comment': 'beta testing'
        })
        query = text("""
            INSERT INTO golden_dataset_hub (golden_dataset_id, golden_dataset_name, created_at, rows, table_path, project_id, golden_dataset_comment, lineage)
            VALUES (:golden_dataset_id, :golden_dataset_name, :created_at, :rows, :table_path, :project_id, :golden_dataset_comment, :lineage);
            """)
            
        connection.execute(query, {
                'golden_dataset_id': "GoldenData" + request.name ,
                'golden_dataset_name': request.name,
                'created_at': '2024-09-04T00:00:00Z',  # Correctly quoted timestamp
                'rows': int(10*len(request.call_ids)*0.1),
                'table_path': '/golden_datasets/datasets',  # Properly formatted array
                'project_id': 'PROJECT01',
                'golden_dataset_comment': 'beta testing',
                'lineage': '{"source": "Merged", "parents": ["GoldenData01", "GoldenData02"]}'

        })

    return CreateNewDatasetResponse(
        new_rows=int(10*len(request.call_ids)*0.9),
        golden_rows=int(10*len(request.call_ids)*0.1)
    )

# Endpoint for update_golden
@app.post("/update_golden", response_model=UpdateGoldenResponse)
async def update_golden(request: UpdateGoldenRequest):
    return UpdateGoldenResponse(
        previous_rows=124,
        new_rows=139
    )

# Running the app
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=9000, 
        ssl_keyfile=r"/home/HrudaiAditya/data_tech/dummy_api/server.key",  # Path to your private key file
        ssl_certfile=r"/home/HrudaiAditya/data_tech/dummy_api/server.crt"  # Path to your certificate file
    )