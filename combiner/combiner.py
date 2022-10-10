from combiner import config as c
from google_cloud_storage_utils import google_cloud_storage_utils as csu
import pandas as pd
from http.client import RemoteDisconnected

def filter_blobs_by_year_month(blob_list, year, month):
    return list(filter(lambda b: ((b.split("-")[1] == month) & (b.split("-")[0][-4:]  == str(year))), blob_list))

def filter_blobs_by_entity(blob_list, network, entity):
    return list(filter(lambda b: (c.BLOB_PATHS[network]["hourly"][entity] in b), blob_list))

def get_dataframe_from_blob(entity, bucket_name, blob_name, token_json_path):
    return pd.read_csv("gs://" + bucket_name + "/" +blob_name,
                       storage_options={"token": token_json_path})[c.ENTITIES[entity]["fields"]]

def combine_data(entities, network, years_list, months_list, bucket, token_json_path, storage_client):
    bucket_name = bucket.name
    all_blobs = csu.get_blob_list(storage_client, bucket)


    for entity in entities:
        for year in years_list:
            for month in months_list:
                data = pd.DataFrame()

                print("entity : " + str(entity) + " : year : " + str(year) + " : month : " + str(month))

                blobs = filter_blobs_by_entity(all_blobs, network, entity)
                blobs = filter_blobs_by_year_month(blobs, year, month)

                for b in blobs:
                    print(b)
                    d = get_dataframe_from_blob(
                        entity,
                        bucket.name,
                        b,
                        token_json_path
                    )
                    data = pd.concat([data, d])
                new_blob_name = c.BLOB_PATHS[network]["monthly"][entity] + str(year) + "/" + str(month) + ".csv"
                flag = 0
                while flag == 0:
                    try:
                        bucket.blob(new_blob_name).upload_from_string(data.to_csv(), 'text/csv')
                        return True
                    except RemoteDisconnected as e:
                        bucket = csu.get_bucket(token_json_path, bucket_name)