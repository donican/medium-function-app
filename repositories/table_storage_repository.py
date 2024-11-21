import logging
from azure.data.tables import TableServiceClient

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class TableDirectoryClient:
    def __init__(self, connection_string: str, table_name: str):
        try:
            self.table_service_client = TableServiceClient.from_connection_string(connection_string)
            self.table_client = self.table_service_client.get_table_client(table_name)
            logger.info(f"Connected to table: {table_name}")
        except Exception as e:
            logger.error(f"Failed to connect: {str(e)}")
            raise