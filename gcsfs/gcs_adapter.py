from google.cloud import storage
import re

class GCSAdapter:
    def __init__(self, project=None, token=None):
        self.client = storage.Client(project=project, credentials=token)

    def _parse_path(self, path):
        match = re.match(r"https://storage.googleapis.com/storage/v1/b/([^/]+)/o/([^?]+)", path)
        if match:
            return match.groups()
        return None, None

    def read_object(self, path, **kwargs):
        bucket_name, blob_name = self._parse_path(path)
        if not bucket_name or not blob_name:
            raise ValueError(f"Invalid GCS path: {path}")

        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        # Assuming the 'read' operation is for downloading the object's content
        try:
            content = blob.download_as_bytes()
            # The original _request method returns a tuple, so we mimic that structure
            # (status, headers, info, contents)
            return 200, {}, {}, content
        except Exception as e:
            # Mimic a basic error response
            return 404, {}, {}, str(e).encode()

    def handle(self, method, path, **kwargs):
        # Check if the request is a read operation.
        # A simple check is to see if the method is 'GET' and if it's a media download link.
        is_read_request = method.upper() == 'GET' and "alt=media" in path
        
        if is_read_request:
            return self.read_object(path, **kwargs)
        
        # If it's not a read request this adapter should handle, do nothing.
        return None