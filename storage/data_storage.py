import os
from typing import Optional

from azure.storage.blob import BlobServiceClient
from utils.logger import get_logger

# Optional (only needed for Managed Identity / Service Principal auth)
try:
    from azure.identity import DefaultAzureCredential
except Exception:
    DefaultAzureCredential = None  # allows local runs without azure-identity installed


def _posix_join(*parts: str) -> str:
    """Join path segments using forward slashes for blob names."""
    return "/".join(p.strip("/\\") for p in parts if p is not None)


class DataStorage:
    """
    ADLS Gen2 upload helper.

    Auth precedence:
      1) Managed Identity / Service Principal via DefaultAzureCredential (if available and account is given)
      2) Account/Key from env: AZURE_STORAGE_ACCOUNT + AZURE_STORAGE_KEY  (fallback)

    Returns abfss:// URIs for convenience, while using the Blob endpoint for uploads.
    """

    def __init__(
        self,
        container_name: str = "climate-data-analysis",
        base_dir: str = "climate_data",
        account_name: Optional[str] = None,
        prefer_managed_identity: bool = True,
    ):
        self.logger = get_logger(self.__class__.__name__)
        self.container_name = container_name
        self.base_dir = base_dir

        # Resolve account name
        acct_env = os.environ.get("AZURE_STORAGE_ACCOUNT")
        self.account_name = account_name or acct_env
        if not self.account_name:
            raise RuntimeError(
                "Storage account name is required. Set AZURE_STORAGE_ACCOUNT or pass account_name."
            )

        account_url = f"https://{self.account_name}.blob.core.windows.net"

        # Choose credential
        self._auth_mode = None
        blob_client_kwargs = {"account_url": account_url}

        if prefer_managed_identity and DefaultAzureCredential is not None:
            try:
                cred = DefaultAzureCredential(
                    exclude_shared_token_cache_credential=True
                )
                # Build client with AAD credential
                self.blob_service_client = BlobServiceClient(credential=cred, **blob_client_kwargs)
                # Force a lightweight call to validate auth (list containers)
                _ = self.blob_service_client.get_service_properties()
                self._auth_mode = "managed_identity"
                self.logger.info("Using Managed Identity / AAD credential for BlobServiceClient.")
            except Exception as e:
                self.logger.warning(
                    f"Managed Identity/AAD auth not available or not permitted; "
                    f"falling back to account/key. Details: {e}"
                )
                self._auth_mode = None

        if self._auth_mode is None:
            # Fallback to Account Key
            key = os.environ.get("AZURE_STORAGE_KEY")
            if not key:
                raise RuntimeError(
                    "No Azure AD credential and no account key found. "
                    "Set APP registration/Managed Identity OR AZURE_STORAGE_KEY."
                )
            self.blob_service_client = BlobServiceClient(
                credential=key, **blob_client_kwargs
            )
            self._auth_mode = "account_key"
            self.logger.info("Using Account Key credential for BlobServiceClient.")

        # Ensure container exists (idempotent)
        try:
            self.blob_service_client.create_container(self.container_name)
            self.logger.info(f"Created container {self.container_name}")
        except Exception:
            self.logger.info(f"Using existing container {self.container_name}")

    def upload(self, local_path: str) -> Optional[str]:
        """
        Upload a local parquet file into Azure Blob Storage under:
          <base_dir>/<year>/CRYYYYMMDD.parquet

        Returns an abfss:// URI for downstream Spark usage.
        """
        try:
            year = os.path.basename(os.path.dirname(local_path))
            blob_name = _posix_join(self.base_dir, year, os.path.basename(local_path))

            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=blob_name,
            )

            with open(local_path, "rb") as data:
                # overwrite=True so re-runs are idempotent
                blob_client.upload_blob(data, overwrite=True)

            self.logger.info(f"Uploaded {local_path} → {blob_name}")

            # Return abfss URI (dfs endpoint) for Spark readers
            abfss_uri = (
                f"abfss://{self.container_name}@{self.account_name}.dfs.core.windows.net/{blob_name}"
            )
            return abfss_uri

        except Exception as e:
            self.logger.error(f"Failed to upload {local_path}: {e}")
            return None
