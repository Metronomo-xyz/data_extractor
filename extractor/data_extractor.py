from extractor import db_connection_utils as dbcu
from google_cloud_storage_utils import google_cloud_storage_utils as gcsu
import pandas as pd
from datetime import timedelta
from time import sleep
import psycopg2 as psy


def create_pandas_table(sql_query, network_creds, db):
    flag = 0
    while flag == 0:
        try:
            table = pd.read_sql_query(sql_query, db)
            return table
        except Exception as e:
            db = dbcu.get_connection(network_creds)


def get_timebounded_query(query, start_time, end_time):
    query_ = query.replace("@start_time", start_time).replace("@end_time", end_time)
    return query_


def extract_data(query, network_creds, start_time, dates_range, bucket, blob_path, data_time_delta=-1, query_sleep_time=5):
    curr_time = start_time
    next_time = curr_time + timedelta(hours=data_time_delta)

    for i in range(24 * dates_range):
        print("getting data from " + str(next_time) + " to " + str(curr_time))
        curr_timestamp = int(round(curr_time.timestamp())) * 1000000000
        next_timestamp = int(round(next_time.timestamp())) * 1000000000

        query_ = get_timebounded_query(query, str(next_timestamp), str(curr_timestamp))
        db = dbcu.get_connection(network_creds)
        data = create_pandas_table(query_, network_creds, db)
        new_blob_name = blob_path + \
            str(next_time).replace(" ", "_").replace(":", "_") + "_" + \
            str(curr_time).replace(" ", "_").replace(":", "_") + ".csv"
        gcsu.write_dataframe_to_blob(data, bucket, new_blob_name)

        curr_time = next_time
        next_time = next_time + timedelta(hours=data_time_delta)

        sleep(query_sleep_time)
