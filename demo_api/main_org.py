# #todo for demo:
# # upload fix -> 
# # create_model simple(hardcode memory required etc.)
# #  playground(inference)
# # CREATE ARENA





# from fastapi import FastAPI, HTTPException, File, UploadFile, Form
# from pydantic import BaseModel
# from typing import List, Dict, Union, Optional
# from fastapi.middleware.cors import CORSMiddleware
# import sqlalchemy
# from openai import OpenAI
# from sqlalchemy import text
# from io import BytesIO
# from logic  import *
# from datetime import datetime

# app = FastAPI()

# # Add CORS middleware
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],  # Allows all origins. Replace with specific origins if needed.
#     allow_credentials=True,
#     allow_methods=["*"],  # Allows all methods. Replace with specific methods if needed.
#     allow_headers=["*"],  # Allows all headers. Replace with specific headers if needed.
# )

# # Model classes for the input and output of APIs
# class MarkDatasetRequest(BaseModel):
#     dataset_id: str
#     dataset_path: str
#     type: str  # new or merged

# class CreateModelRequest(BaseModel):
#     training_mode: str  # auto or manual
#     dataset: str
#     eval_metric: str
#     config_file: UploadFile

# class DeleteDeployArchiveModelRequest(BaseModel):
#     model_id: str
#     meta: Optional[str] = None  # JSON string

# class ModelResponse(BaseModel):
#     model_name: str
#     base_model: str
#     dataset: str
#     eval_metrics: str
#     performance: float
#     status: str

# class Message(BaseModel):
#     role: str
#     content: str

# class ModelInferenceRequest(BaseModel):
#     model: str
#     messages: List[Message]
#     stream: bool
#     meta: Optional[str] = None

# class AddArenaRequest(BaseModel):
#     model1: str
#     model2: str

# class AddGoldenRequest(BaseModel):
#     model: str
#     ref_model_1: str
#     ref_model_2: str
#     ref_model_3: str
#     metric_1: str
#     metric_2: str
#     metric_3: str

# class ModelInferenceResponse(BaseModel):
#     output: str
#     status: str

# class AddArenaResponse(BaseModel):
#     status: str
#     message: str

# class AddGoldenResponse(BaseModel):
#     status: str
#     message: str


# # Request models
# class MergeGoldenRequest(BaseModel):
#     call_ids: List[str]
#     source_dataset_id: Optional[str] = None
#     name: str
#     comments: Optional[str] = None  # Made optional

# class MergeDatasetRequest(BaseModel):
#     call_ids: List[str]
#     source_dataset_id: Optional[str] = None
#     destination_dataset_id: str
#     name: str
#     comments: Optional[str] = None  # Made optional

# class CreateNewDatasetRequest(BaseModel):
#     call_ids: List[str]
#     source_dataset_id: Optional[str] = None
#     name: str
#     comments: Optional[str] = None  # Made optional

# class UpdateGoldenRequest(BaseModel):
#     omitted_call_ids: List[str]
#     comments: Optional[str] = None  # Made optional

# # Dummy Response models
# class MergeGoldenResponse(BaseModel):
#     previous_rows: int
#     new_rows: int

# class MergeDatasetResponse(BaseModel):
#     new_rows: int
#     golden_rows: int

# class CreateNewDatasetResponse(BaseModel):
#     new_rows: int
#     golden_rows: int

# class UpdateGoldenResponse(BaseModel):
#     previous_rows: int
#     new_rows: int

# class RefineDataRequest(BaseModel):
#     dataset_id = str

# def connect_tcp_socket() -> sqlalchemy.engine.base.Engine:
#     """Initializes a TCP connection pool for a Cloud SQL instance of Postgres."""

#     pool = sqlalchemy.create_engine(
#         # Equivalent URL:
#         # url = 'genloopv1:us-central1:genloop-postgresv1'
#         # url = 'postgresql+psycopg2://postgres:genloop@123@34.68.9.168:5302/postgresv1'
#         sqlalchemy.engine.url.URL.create(
#             drivername="postgresql+psycopg2",
#             username = "postgres",
#             password='genloop@123', #TODO pick from env
#             host='34.68.9.168', #TODO pick from env
#             port='5432',
#             database='postgres',
#         )
#     )
#     return pool

# # Dummy in-memory storage
# models_db = {}
# datasets_db = {}

# # remove
# @app.post("/mark_dataset") 
# async def mark_dataset(request: MarkDatasetRequest):
#     return {
#         "is_success": True,
#         "golden_data_entries": 100,  # Dummy data
#         "dataset_entries": 95  # Dummy data
#     }

# @app.post("/refine_data")
# async def refine_data_response(request: RefineDataRequest):
#     return {"HTTP Status code": 200}


# @app.post("/upload_dataset")
# async def upload_dataset_response(
#     name: str = Form(...), 
#     comment: str = Form(None),
#     file: UploadFile = File(...)):
#     if file:
#         content = await file.read()
#         print(f"File received with content length: {len(content)}")
        
#         # Use BytesIO to read the contents as a CSV
#         df = pd.read_csv(BytesIO(content))
        
#         engine = connect_tcp_socket()

#         # make another df with 10% of len(df) -> delete those from df (90% left) -> insert it goldens_schema & golden_hub

#         #entry to datasets schema
#         create_table_from_csv_raw(engine, "datasets",name, df)

#         dataset_id = name # temporary
#         dataset_name  = name
#         created_at  = datetime.now()
#         rows = len(df)
#         # entry to datasethub
#         insert_dataset_entry(engine, dataset_id, dataset_name, comment, created_at, rows)

#     return {
#         "status": "success",
#         "message": f"data uploaded."
#     }

# @app.post("/create_model")
# async def create_model(
#     training_mode: str = Form(...), 
#     dataset : str = Form(...),
#     eval_metric: str = Form(...),
#     data_file: UploadFile = File(None)
# ):

#     # model_id = "model_" + str(len(models_db) + 1)
#     # models_db[model_id] = {
#     #     "model_name": model_id,
#     #     "base_model": "base_model_1",
#     #     "dataset": request.dataset,
#     #     "eval_metrics": request.eval_metric,
#     #     "performance": 0.85,
#         # "status": "created"
#     # }
#     # connection = connect_tcp_socket()
    
# #     query = text("""
# #     INSERT INTO model_hub (training_mode, dataset, model_status)
# # VALUES (value1, value2, value3,);

# #     """)
    
# #     result = connection.execute(query, {
# #         "type": dataset_type,
# #         "is_golden_dataset": is_golden_dataset
# #     })
    
#     # return [row[0] for row in result]
#     return {"HTTP Status code": 200}

# @app.post("/delete_model")
# async def delete_model(request: DeleteDeployArchiveModelRequest):
#     model_id = request.model_id
    
#     return {"HTTP Status code": 200}
    
# @app.post("/deploy_model")
# async def deploy_model(request: DeleteDeployArchiveModelRequest):
    
#     return {"HTTP Status code": 200}

# @app.post("/test_model")
# async def test_model(request: DeleteDeployArchiveModelRequest):
    
#     return {"HTTP Status code": 200}
    

# @app.post("/archive_model")
# async def archive_model(request: DeleteDeployArchiveModelRequest):
#     model_id = request.model_id
    
#     return {"HTTP Status code": 200}
    

# @app.post("/model_inference", response_model=ModelInferenceResponse)
# async def model_inference(request: ModelInferenceRequest):
#     # Dummy implementation

#     try:
#         input = request.messages[-1].content
#         openai_api_key = "LOAD FROM ENV"

#         client = OpenAI(
#             api_key=openai_api_key
#         )

#         response = client.chat.completions.create(
#         model = "gpt-4o-mini",
#         messages = [ 
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
#             "output": "This is a dummy output",
#             "status": "success"
#         }


# @app.post("/add_arena", response_model=AddArenaResponse)
# async def add_arena(
#     model1: str = Form(...), 
#     model2: str = Form(...),
#     file: UploadFile = File(...)
# ):
    
#     if file:
#         content = await file.read()
#         print(f"File received with content length: {len(content)}")

#     return {
#         "status": "success",
#         "message": f"Arena created with models '{model1}' and '{model2}', and file uploaded."
#     }
    

# @app.post("/add_golden", response_model=AddGoldenResponse)
# async def add_golden(request: AddGoldenRequest):
#     # Dummy implementation
#     return {
#         "status": "success",
#         "message": "Golden data added successfully."
#     }

# @app.post("/merge_golden", response_model=MergeGoldenResponse)
# async def merge_golden(request: MergeGoldenRequest):
#     return MergeGoldenResponse(
#         previous_rows= 143,
#         new_rows=164
#     )

# # Endpoint for merge_dataset
# @app.post("/merge_dataset", response_model=MergeDatasetResponse)
# async def merge_dataset(request: MergeDatasetRequest):
#     engine = connect_tcp_socket()
#     with engine.connect() as connection:
#         connection.execution_options(isolation_level="AUTOCOMMIT")
#         query = text("""
#             INSERT INTO dataset_hub (dataset_id, dataset_name, created_at, rows, model_ids, project_id, dataset_comment)
#             VALUES (:dataset_id, :dataset_name, :created_at, :rows, :model_ids, :project_id, :dataset_comment);
#             """)
            
#         connection.execute(query, {
#                 'dataset_id': request.name ,
#                 'dataset_name': request.name,
#                 'created_at': '2024-09-04T00:00:00Z',  # Correctly quoted timestamp
#                 'rows': int(10*len(request.call_ids)*0.9),
#                 'model_ids': '{}',  # Properly formatted array
#                 'project_id': 'PROJECT01',
#                 'dataset_comment': 'beta testing'
#         })
#         #golden dataset
#         query = text("""
#             INSERT INTO golden_dataset_hub (golden_dataset_id, golden_dataset_name, created_at, rows, table_path, project_id, golden_dataset_comment, lineage)
#             VALUES (:golden_dataset_id, :golden_dataset_name, :created_at, :rows, :table_path, :project_id, :golden_dataset_comment, :lineage);
#             """)
            
#         connection.execute(query, {
#                 'golden_dataset_id': "GoldenData" + request.name ,
#                 'golden_dataset_name': request.name,
#                 'created_at': '2024-09-04T00:00:00Z',  # Correctly quoted timestamp
#                 'rows': int(10*len(request.call_ids)*0.1),
#                 'table_path': '/golden_datasets/datasets',  # Properly formatted array
#                 'project_id': 'PROJECT01',
#                 'golden_dataset_comment': 'beta testing',
#                 'lineage': '{"source": "Merged", "parents": ["GoldenData01", "GoldenData02"]}'
#         })
#     return MergeDatasetResponse(
#         new_rows=int(10*len(request.call_ids)*0.9),
#         golden_rows=int(10*len(request.call_ids)*0.1)
#     )

# # Endpoint for create_new_dataset
# @app.post("/create_new_dataset", response_model=CreateNewDatasetResponse)
# async def create_new_dataset(request: CreateNewDatasetRequest):

#     engine = connect_tcp_socket()
#     with engine.connect() as connection:
#         connection.execution_options(isolation_level="AUTOCOMMIT")
#         query = text("""
#             INSERT INTO dataset_hub (dataset_id, dataset_name, created_at, rows, model_ids, project_id, dataset_comment)
#             VALUES (:dataset_id, :dataset_name, :created_at, :rows, :model_ids, :project_id, :dataset_comment);
#             """)
            
#         connection.execute(query, {
#                 'dataset_id': request.name ,
#                 'dataset_name': request.name,
#                 'created_at': '2024-09-04T00:00:00Z',  # Correctly quoted timestamp
#                 'rows': int(10*len(request.call_ids)*0.9),
#                 'model_ids': '{}',  # Properly formatted array
#                 'project_id': 'PROJECT01',
#                 'dataset_comment': 'beta testing'
#         })
#         query = text("""
#             INSERT INTO golden_dataset_hub (golden_dataset_id, golden_dataset_name, created_at, rows, table_path, project_id, golden_dataset_comment, lineage)
#             VALUES (:golden_dataset_id, :golden_dataset_name, :created_at, :rows, :table_path, :project_id, :golden_dataset_comment, :lineage);
#             """)
            
#         connection.execute(query, {
#                 'golden_dataset_id': "GoldenData" + request.name ,
#                 'golden_dataset_name': request.name,
#                 'created_at': '2024-09-04T00:00:00Z',  # Correctly quoted timestamp
#                 'rows': int(10*len(request.call_ids)*0.1),
#                 'table_path': '/golden_datasets/datasets',  # Properly formatted array
#                 'project_id': 'PROJECT01',
#                 'golden_dataset_comment': 'beta testing',
#                 'lineage': '{"source": "Merged", "parents": ["GoldenData01", "GoldenData02"]}'

#         })

#     return CreateNewDatasetResponse(
#         new_rows=int(10*len(request.call_ids)*0.9),
#         golden_rows=int(10*len(request.call_ids)*0.1)
#     )

# # Endpoint for update_golden
# @app.post("/update_golden", response_model=UpdateGoldenResponse)
# async def update_golden(request: UpdateGoldenRequest):
#     return UpdateGoldenResponse(
#         previous_rows=124,
#         new_rows=139
#     )

# # Running the app
# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=8000)
