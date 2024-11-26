import logging
from azure.data.tables import TableServiceClient
from azure.core.exceptions import AzureError, ResourceNotFoundError, ClientAuthenticationError, HttpResponseError
from config import Config
from typing import List, Dict 

logger = logging.getLogger(__name__)

class TableDirectoryClient:
    def __init__(self, table_name: str):
        try:
            self.table_service_client = TableServiceClient.from_connection_string(Config.AZURE_STORAGE_CONNECTION_STRING)
            self.table_client = self.table_service_client.get_table_client(table_name)
            logger.info(f"Connected to table: {table_name}")
        except Exception as e:
            logger.error(f"Failed to connect to table {table_name}: {str(e)}")
            raise
    
    @staticmethod
    def build_table_storage_filter(partition_key: str = None, row_key: str = None) -> str:
        """
        Constructs a filter query for Azure Table Storage based on PartitionKey and/or RowKey.

        :param partition_key: PartitionKey for filtering.
        :param row_key: RowKey for filtering.
        :return: OData filter query string.
        """
        if partition_key and row_key:
            return f"PartitionKey eq '{partition_key}' and RowKey eq '{row_key}'"
        elif partition_key:
            return f"PartitionKey eq '{partition_key}'"
        elif row_key:
            return f"RowKey eq '{row_key}'"
        return None


    def read_entities(self, partition_key: str = None, row_key: str = None) -> List[Dict]:
        """
        Reads entities from the Azure Table Storage.

        :param partition_key: PartitionKey for filtering entities.
        :param row_key: RowKey for filtering entities.
        :return: List of entities retrieved from the table.
        :raises: ResourceNotFoundError, ClientAuthenticationError, HttpResponseError, AzureError, Exception.
        """
        try:
            # Build the filter query dynamically using the helper function
            filter_query = self.build_table_storage_filter(partition_key, row_key)

            if filter_query:
                logger.info(f"Reading entities with filter: {filter_query}")
                entities = self.table_client.query_entities(filter_query)
            else:
                logger.info("Reading all entities from the table.")
                entities = self.table_client.list_entities()

            return [entity for entity in entities]
        
        except ResourceNotFoundError as e:
            logger.error(f"Resource not found: {str(e)}")
            raise  
        except ClientAuthenticationError as e:
            logger.error(f"Authentication failed: {str(e)}")
            raise
        except HttpResponseError as e:
            logger.error(f"HTTP error occurred: {str(e)}")
            raise
        except AzureError as e:
            logger.error(f"Azure error occurred: {str(e)}")
            raise 
        except Exception as e:
            logger.error(f"Unexpected error occurred: {str(e)}")
            raise

    def insert_entity(self, entity: Dict) -> None:
        """
        Inserts an entity into the Azure Table Storage.

        :param entity: A dictionary representing the entity to insert. 
                    Must contain 'PartitionKey' and 'RowKey'.
        :raises: ResourceNotFoundError, ClientAuthenticationError, HttpResponseError, AzureError, Exception.
        """
        try:
            # Verifica se 'PartitionKey' e 'RowKey' estão presentes
            if 'PartitionKey' not in entity or 'RowKey' not in entity:
                raise ValueError("The entity must contain 'PartitionKey' and 'RowKey'.")

            logger.info(f"Inserting entity: {entity}")
            self.table_client.create_entity(entity=entity)
            logger.info("Entity inserted successfully.")
        
        except ResourceNotFoundError as e:
            logger.error(f"Table not found: {str(e)}")
            raise
        except ClientAuthenticationError as e:
            logger.error(f"Authentication failed: {str(e)}")
            raise
        except HttpResponseError as e:
            logger.error(f"HTTP error occurred: {str(e)}")
            raise
        except AzureError as e:
            logger.error(f"Azure error occurred: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error occurred: {str(e)}")
            raise

    def insert_entities(self, entities: List[Dict]) -> None:
        """
        Inserts multiple entities into the Azure Table Storage.

        :param entities: A list of dictionaries representing the entities to insert.
                        Each entity must contain 'PartitionKey' and 'RowKey'.
        :raises: ResourceNotFoundError, ClientAuthenticationError, HttpResponseError, AzureError, Exception.
        """
        try:
            for entity in entities:
                # Verifica se 'PartitionKey' e 'RowKey' estão presentes
                if 'PartitionKey' not in entity or 'RowKey' not in entity:
                    raise ValueError(f"Each entity must contain 'PartitionKey' and 'RowKey'. Invalid entity: {entity}")

                logger.info(f"Inserting entity: {entity}")
                self.table_client.create_entity(entity=entity)
            
            logger.info(f"Successfully inserted {len(entities)} entities.")

        except ResourceNotFoundError as e:
            logger.error(f"Table not found: {str(e)}")
            raise
        except ClientAuthenticationError as e:
            logger.error(f"Authentication failed: {str(e)}")
            raise
        except HttpResponseError as e:
            logger.error(f"HTTP error occurred: {str(e)}")
            raise
        except AzureError as e:
            logger.error(f"Azure error occurred: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error occurred: {str(e)}")
            raise
