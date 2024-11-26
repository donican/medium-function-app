import azure.functions as func
import logging
from repositories.table_storage_repository import TableDirectoryClient
from logger_config import setup_logging
import logging

setup_logging()
logger = logging.getLogger(__name__)

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="medium_http_trigger")
def medium_http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    table_name = "ActivityLogs"
    table_client = TableDirectoryClient(table_name)
    
    single_entity = {
        "PartitionKey": "TestPartition",
        "RowKey": "1",
        "UserId": "3",
        "Action": "SingleInsertTest",
        "Timestamp": "2024-11-24T12:00:00Z"
    }

    try:
        print("Testing single entity insertion...")
        table_client.insert_entity(single_entity)
        print("Single entity inserted successfully!")
    except Exception as e:
        print(f"Error inserting single entity: {str(e)}")

    multiple_entities = [
        {
            "PartitionKey": "TestPartition",
            "RowKey": "2",
            "UserId": "3",
            "Action": "BulkInsertTest1",
            "Timestamp": "2024-11-24T12:05:00Z"
        },
        {
            "PartitionKey": "TestPartition",
            "RowKey": "3",
            "UserId": "3",
            "Action": "BulkInsertTest2",
            "Timestamp": "2024-11-24T12:10:00Z"
        },
    ]

    try:
        print("Testing multiple entities insertion...")
        table_client.insert_entities(multiple_entities)
        print("Multiple entities inserted successfully!")
    except Exception as e:
        print(f"Error inserting multiple entities: {str(e)}")



    entities = table_client.read_entities()

    if entities:
        for entity in entities:
            logging.info(f"Entidade: {entity}")
    else:
        logging.info("No entities found or an error occurred.")

    return func.HttpResponse(
        "Entities read successfully! Check logs for more details.",
        status_code=200
    )