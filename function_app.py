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