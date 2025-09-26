import os
from hdfs import InsecureClient
from utils.logger import get_logger

class DataStorage:
    def __init__(self, hdfs_url, hdfs_dir):
        self.client = InsecureClient(hdfs_url)
        self.hdfs_dir = hdfs_dir
        self.logger = get_logger(self.__class__.__name__)
        self.client.makedirs(self.hdfs_dir)

    def upload(self, local_path):
        try:
            # Extract year from local path (directory above the file)
            year = os.path.basename(os.path.dirname(local_path))

            # Create year folder in HDFS
            hdfs_year_dir = os.path.join(self.hdfs_dir, year)
            self.client.makedirs(hdfs_year_dir)

            # Upload into year directory
            hdfs_path = os.path.join(hdfs_year_dir, os.path.basename(local_path))
            self.client.upload(hdfs_path, local_path, overwrite=True)

            self.logger.info(f"Uploaded {local_path} → {hdfs_path}")
            return hdfs_path

        except Exception as e:
            self.logger.error(f"Failed to upload {local_path}: {e}")
            return None
