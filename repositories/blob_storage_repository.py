import logging
from azure.storage.blob import BlobServiceClient, ContainerClient
from azure.core.exceptions import AzureError, ResourceNotFoundError, ClientAuthenticationError, HttpResponseError
from config import Config

logger = logging.getLogger(__name__)

class BlobStorageClient:
    def __init__(self, container_name: str):
        try:
            # Cria o cliente de serviço do Blob Storage
            self.blob_service_client = BlobServiceClient.from_connection_string(Config.AZURE_STORAGE_CONNECTION_STRING)
            
            # Obtém o cliente para o container especificado
            self.container_client = self.blob_service_client.get_container_client(container_name)
            
            # Verifica se o container existe
            if not self.container_client.exists():
                raise ResourceNotFoundError(f"Container '{container_name}' does not exist.")
            
            logger.info(f"Connected to blob container: {container_name}")
        except (AzureError, ClientAuthenticationError, HttpResponseError, ResourceNotFoundError) as e:
            logger.error(f"Failed to connect to blob container '{container_name}': {str(e)}")
            raise

    def upload_blob(self, blob_name: str, data: bytes) -> None:
        """Faz o upload de um blob."""
        try:
            self.container_client.upload_blob(name=blob_name, data=data, overwrite=True)
            logger.info(f"Uploaded blob: {blob_name}")
        except AzureError as e:
            logger.error(f"Failed to upload blob '{blob_name}': {str(e)}")
            raise

    def download_blob(self, blob_name: str) -> bytes:
        """Faz o download de um blob."""
        try:
            blob_client = self.container_client.get_blob_client(blob_name)
            blob_data = blob_client.download_blob().readall()
            logger.info(f"Downloaded blob: {blob_name}")
            return blob_data
        except AzureError as e:
            logger.error(f"Failed to download blob '{blob_name}': {str(e)}")
            raise

    def list_blobs(self) -> List[str]:
        """Lista todos os blobs no container."""
        try:
            blobs = [blob.name for blob in self.container_client.list_blobs()]
            logger.info(f"Listed blobs in container: {blobs}")
            return blobs
        except AzureError as e:
            logger.error(f"Failed to list blobs: {str(e)}")
            raise
