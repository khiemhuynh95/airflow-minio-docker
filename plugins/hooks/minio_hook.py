from minio import Minio
from airflow.hooks.base import BaseHook
import json
class MinioHook:
    def __init__(self, connection_id: str, secure: bool = False):
        self.connection_id = connection_id
        self.secure = secure
        self.client = self._get_minio_client()

    def _get_minio_client(self):
        # Fetch the Minio connection details from Airflow
        connection = BaseHook.get_connection(self.connection_id)
        
        # Parse endpoint
        url_prefix = 'https://' if self.secure else 'http://'
        host = json.loads(connection.get_extra())['endpoint_url'].replace(url_prefix, '')

        # Minio client configuration using the connection details
        minio_client = Minio(
            endpoint=host,
            access_key=connection.login,
            secret_key=connection.password,
            secure=self.secure
        )

        return minio_client

    def test_connection(self):
        try:
            # Test the connection by listing buckets
            self.client.list_buckets()
            return True
        except Exception as e:
            print(f"Failed to connect to Minio: {e}")
            return False

    def download_file(self, bucket_name: str, object_name: str, local_file_path: str):
        try:
            self.client.fget_object(bucket_name, object_name, local_file_path)
            print(f"File {object_name} from bucket {bucket_name} downloaded to {local_file_path}")
        except Exception as e:
            print(f"Failed to download file: {e}")
            raise

# Example usage:
# minio_hook = MinioHook(endpoint='host.docker.internal:9000', access_key='your-access-key', secret_key='your-secret-key')
# if minio_hook.test_connection():
#     minio_hook.download_file('airflow', 'Orders.csv', '/tmp/Orders.csv')
