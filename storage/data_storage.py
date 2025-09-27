import os
from azure.storage.blob import BlobServiceClient
from utils.logger import get_logger


class DataStorage:
    def __init__(self, container_name="climate-data-analysis", base_dir="climate_data"):
        """
        Initializes connection to Azure Blob Storage.
        Requires environment variables:
          AZURE_STORAGE_ACCOUNT
          AZURE_STORAGE_KEY
        """
        account = os.environ["AZURE_STORAGE_ACCOUNT"]
        key = os.environ["AZURE_STORAGE_KEY"]

        account_url = f"https://{account}.blob.core.windows.net"
        self.blob_service_client = BlobServiceClient(
            account_url=account_url,
            credential=key
        )

        self.container_name = container_name
        self.base_dir = base_dir
        self.logger = get_logger(self.__class__.__name__)

        # Ensure container exists
        try:
            self.blob_service_client.create_container(self.container_name)
            self.logger.info(f"Created container {self.container_name}")
        except Exception:
            self.logger.info(f"Using existing container {self.container_name}")

    def upload(self, local_path):
        """
        Upload a local parquet file into Azure Blob Storage under:
          abfss://<container>@<account>.dfs.core.windows.net/<base_dir>/<year>/CRYYYYMMDD.parquet
        """
        try:
            year = os.path.basename(os.path.dirname(local_path))
            blob_path = os.path.join(self.base_dir, year, os.path.basename(local_path))

            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=blob_path
            )

            with open(local_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)

            self.logger.info(f"Uploaded {local_path} → {blob_path}")
            return f"abfss://{self.container_name}@{os.environ['AZURE_STORAGE_ACCOUNT']}.dfs.core.windows.net/{blob_path}"

        except Exception as e:
            self.logger.error(f"Failed to upload {local_path}: {e}")
            return None
