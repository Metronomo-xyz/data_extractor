from aggregator import config as c
import pandas as pd
from google_cloud_storage_utils import google_cloud_storage_utils as csu
from datetime import datetime, timedelta


def get_dataframe_from_blob(entity, bucket_name, blob_name, token_json_path, extended_data=False):
    if (extended_data):
        return pd.read_csv("gs://" + bucket_name + "/" + blob_name,
                           storage_options={"token": token_json_path})[c.ENTITIES[entity]["fields_extended"]]
    else:
        return pd.read_csv("gs://" + bucket_name + "/" + blob_name,
                       storage_options={"token": token_json_path})[c.ENTITIES[entity]["fields"]]

def filter_blobs_by_year_month(blob_list, year, month):
    return list(filter(lambda b: ((b.split("-")[1] == month) & (b.split("-")[0][-4:]  == str(year))), blob_list))

def filter_blobs_by_entity(blob_list, network, entity):
    return list(filter(lambda b: (c.BLOB_PATHS[network]["hourly"][entity] in b), blob_list))

def filter_blobs_by_date(blob_list, date, entity):
    if ("action_receipt_actions" in blob_list[0]):
        return list(filter(lambda b: b.split("_")[3].split("/")[1] == date, blob_list))
    else:
        return list(filter(lambda b: b.split("_")[1].split("/")[2] == date, blob_list))

def aggregate_daily(entities, storage_client, bucket, network, dates, token_json_path, extended_data=False):
    if (len(dates) == 0):
        dates = [datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')]

    all_blobs = csu.get_blob_list(storage_client, bucket)

    for e in entities:
        print("entity : " + str(e))
        entity_blobs = filter_blobs_by_entity(all_blobs, network, e)

        for d in dates:
            print("date : " + str(d))
            daily_df = pd.DataFrame()
            daily_blobs = filter_blobs_by_year_month(entity_blobs, d.split("-")[0],d.split("-")[1])
            daily_blobs = filter_blobs_by_date(daily_blobs, d, e)
            print("starting dates")
            for b in daily_blobs:
                print(b)
                df = get_dataframe_from_blob(
                    e,
                    bucket.name,
                    b,
                    token_json_path,
                    extended_data
                )
                if (e == "actions"):
                    df = df[df["action_kind"] == "FUNCTION_CALL"]
                if (extended_data):
                    df = df[c.ENTITIES[e]["fields_extended"]]
                else:
                    df = df[c.ENTITIES[e]["fields"]]
                daily_df = pd.concat([daily_df, df])
            if (extended_data):
                new_blob_name = c.BLOB_PATHS[network]["daily_extended"][e] + d
            else:
                new_blob_name = c.BLOB_PATHS[network]["daily"][e] + d
            print("new blob name : " + new_blob_name)
            csu.write_dataframe_to_blob(daily_df, bucket, new_blob_name)



