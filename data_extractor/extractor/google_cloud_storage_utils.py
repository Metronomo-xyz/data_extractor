from google.cloud import storage

def get_bucket(token_json_path, bucket_name):
    storage_client = storage.Client.from_service_account_json(token_json_path)
    root_bucket = storage_client.bucket(bucket_name)

    return root_bucket

def write_data_to_blob(data, bucket, blob_name):
    if (bucket.blob(blob_name).exists()):
        bucket.blob(blob_name).delete()

    bucket.blob(blob_name).upload_from_string(data.to_csv(), 'text/csv')

    return 0