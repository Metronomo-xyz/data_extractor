from google.cloud import storage
import pandas as pd


def get_bucket(token_json_path, bucket_name):
    storage_client = storage.Client.from_service_account_json(token_json_path)
    root_bucket = storage_client.bucket(bucket_name)
    return root_bucket

def get_blob_list(storage_client, bucket_obj):
    def get_blob_name(blob):
        return blob.name
    return list(map(get_blob_name, list(storage_client.list_blobs(bucket_obj))))

def write_dataframe_to_blob(data_frame, bucket, blob_name):
    if (bucket.blob(blob_name).exists()):
        bucket.blob(blob_name).delete()
    bucket.blob(blob_name).upload_from_string(data_frame.to_csv(), 'text/csv')
    return 0

def filter_blobs_by_year_month(blob_list, year, month):
    return list(filter(lambda b: ((b.split("-")[1] == month) & (b.split("-")[0][-4:]  == str(year))), blob_list))

def filter_blobs_by_path(blob_list, path):
    return list(filter(lambda b: path in b, blob_list))

def filter_blobs_by_date(blob_list, date):
    return list(filter(lambda b: b.split("_")[1].split("/")[2] == date, blob_list))

def get_dataframe_from_blob(bucket, blob_name, fields, token_json_path):
    return pd.read_csv("gs://" + bucket.name + "/" + blob_name,
                       storage_options={"token": token_json_path})[fields]

def filter_blobs_by_dates(blob_list, dates_arr):
    def dateToStr(d):
        return d.strftime("%Y-%m-%d")

    dates = list(map(dateToStr, dates_arr))
    if ("action_receipt_actions" in blob_list[0]):
        return list(filter(lambda b: b.split("_")[3].split("/")[1] in dates, blob_list))
    if ("transactions" in blob_list[0]):
        return list(filter(lambda b: b.split("_")[1].split("/")[2] in dates, blob_list))
