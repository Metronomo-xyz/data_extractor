from google.cloud import storage


def get_bucket(token_json_path, bucket_name):
    storage_client = storage.Client.from_service_account_json(token_json_path)
    root_bucket = storage_client.bucket(bucket_name)
    return root_bucket


def get_blob_name(blob):
    return blob.name


def get_blob_list(token_json_path, bucket):
    storage_client = storage.Client.from_service_account_json(token_json_path)
    return list(map(get_blob_name, list(storage_client.list_blobs(bucket))))


def get_blob_list(storage_client, bucket):
    return list(map(get_blob_name, list(storage_client.list_blobs(bucket))))


def write_dataframe_to_blob(data_frame, bucket, blob_name):
    if (bucket.blob(blob_name).exists()):
        bucket.blob(blob_name).delete()
    bucket.blob(blob_name).upload_from_string(data_frame.to_csv(), 'text/csv')
    return 0