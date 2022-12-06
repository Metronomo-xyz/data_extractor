import combiner.config as c
from google_cloud_storage_utils import google_cloud_storage_utils as csu
import pandas as pd
from http.client import RemoteDisconnected
import time
from itertools import combinations
import sys
from google.oauth2 import service_account


def combine_data(network, dates, bucket, token_json_path, storage_client, how="daily"):
    bucket_name = bucket.name
    all_blobs = csu.get_blob_list(storage_client, bucket)

    ara_blobs = csu.filter_blobs_by_path(all_blobs, c.BLOB_PATHS[network][how]["actions"])


    ara_blobs = csu.filter_blobs_by_dates(ara_blobs, dates)
    print(ara_blobs)

    tx_blobs = csu.filter_blobs_by_path(all_blobs, c.BLOB_PATHS[network][how]["transactions"])
    tx_blobs = csu.filter_blobs_by_dates(tx_blobs, dates)
    print(tx_blobs)

    tx_df = pd.DataFrame()
    for tx_b in tx_blobs:
        print(tx_b)
        tx_data = csu.get_dataframe_from_blob(
            bucket,
            tx_b,
            c.ENTITIES["transactions"]["fields"],
            token_json_path
        )
        tx_data = tx_data[["signer_account_id", "receiver_account_id", "converted_into_receipt_id"]]
        tx_df = pd.concat([tx_df, tx_data])
        tx_df = tx_df.drop_duplicates()

    ara_df = pd.DataFrame()
    for ara_b in ara_blobs:
        print(ara_b)
        ara_data = csu.get_dataframe_from_blob(
            bucket,
            ara_b,
            c.ENTITIES["actions"]["fields"],
            token_json_path
        )
        ara_data = ara_data[ara_data["action_kind"] == "FUNCTION_CALL"]
        ara_data = ara_data[["receipt_id"]]
        ara_df = pd.concat([ara_df, ara_data])
        ara_df = ara_df.drop_duplicates()

    print("creating join")
    j = tx_df.set_index("converted_into_receipt_id").join(
        ara_df.set_index("receipt_id"), how="inner"
    )[["signer_account_id", "receiver_account_id"]].reset_index(drop=True).drop_duplicates()

    a = j.groupby(by="receiver_account_id").count().reset_index()

    print(a.head())

    print("joined")
    print("j size : " + str(sys.getsizeof(j) / 1024 / 1024 / 1024))

    del tx_df
    del ara_df

    def calculateAB(f):
        if (len(f["receiver_account_id"]) == 1):
            return
        l = f["receiver_account_id"].to_list()
        res = list(combinations(l, 2))
        res = [list(x) for x in res]
        return res

    print("creating tmp")
    j = j.groupby(by="signer_account_id").apply(calculateAB).dropna().reset_index().explode(0)
    print("j size : " + str(sys.getsizeof(j) / 1024 / 1024 / 1024))
    j[['sm1', 'sm2']] = pd.DataFrame(j[0].tolist(), index=j.index)
    print("j size : " + str(sys.getsizeof(j) / 1024 / 1024 / 1024))
    j = j[["signer_account_id", "sm1", "sm2"]]
    print("j size : " + str(sys.getsizeof(j) / 1024 / 1024 / 1024))
    j = j.set_index(["sm1", "sm2"])
    print("j size : " + str(sys.getsizeof(j) / 1024 / 1024 / 1024))

    print("creating result")
    j = j.groupby(level=[0,1]).count().reset_index()
    print("j size : " + str(sys.getsizeof(j) / 1024 / 1024 / 1024))

    project_name = 'web3advertisement'
    dataset_name = 'test_data'
    credentials = service_account.Credentials.from_service_account_file('../web3advertisement-94ba21675884.json')

    table_name = 'project_users'
    a.to_gbq(destination_table='{}.{}'.format(dataset_name, table_name), project_id=project_name, if_exists='replace', credentials=credentials)

    table_name = 'project_users_intersection'
    j.to_gbq(destination_table='{}.{}'.format(dataset_name, table_name), project_id=project_name, if_exists='replace', credentials=credentials)



