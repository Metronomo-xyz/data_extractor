import combiner.config as c
from google_cloud_storage_utils import google_cloud_storage_utils as csu
import pandas as pd
from http.client import RemoteDisconnected
import time
from itertools import combinations
import sys
from google.oauth2 import service_account

def filter_blobs_by_year_month(blob_list, year, month):
    return list(filter(lambda b: ((b.split("-")[1] == month) & (b.split("-")[0][-4:]  == str(year))), blob_list))

def filter_blobs_by_entity(blob_list, network, entity, how):
    return list(filter(lambda b: (c.BLOB_PATHS[network][how][entity] in b), blob_list))

def filter_blobs_by_date(blob_list, date):
    return list(filter(lambda b: b.split("_")[1].split("/")[2] == date, blob_list))

def get_dataframe_from_blob(entity, bucket_name, blob_name, token_json_path):
    return pd.read_csv("gs://" + bucket_name + "/" + blob_name,
                       storage_options={"token": token_json_path})[c.ENTITIES[entity]["fields"]]

def filter_blobs_by_date(blob_list, date):
    if ("action_receipt_actions" in blob_list[0]):
        return list(filter(lambda b: b.split("_")[3].split("/")[1] == date, blob_list))
    return list(filter(lambda b: b.split("_")[1].split("/")[2] == date, blob_list))

def combine_data(entities, network, years_list, months_list, bucket, token_json_path, storage_client, how="daily"):
    bucket_name = bucket.name
    all_blobs = csu.get_blob_list(storage_client, bucket)

    ara_blobs = filter_blobs_by_entity(all_blobs, network, "actions", how)
    a_blobs = list()

    # TODO: change hardcode days
    for i in range(32):
        for m in ["09", "10", "11"]:
            date = "2022-" + m + "-"
            if (i < 10):
                date = date + "0" + str(i)
            else:
                date = date + str(i)
            a_blobs = a_blobs + filter_blobs_by_date(ara_blobs, date)
    print(a_blobs)

    tx_blobs = filter_blobs_by_entity(all_blobs, network, "transactions", how)
    t_blobs = list()
    #TODO: change hardcode days
    for i in range(32):
        for m in ["09", "10", "11"]:
            date = "2022-" + m + "-"
            if (i < 10):
                date = date + "0" + str(i)
            else:
                date = date + str(i)
            t_blobs = t_blobs + filter_blobs_by_date(tx_blobs, date)
    print(t_blobs)

    tx_df = pd.DataFrame()
    for tx_b in t_blobs:
        print(tx_b)
        t0 = time.time()
        tx_data = get_dataframe_from_blob(
            "transactions",
            bucket.name,
            tx_b,
            token_json_path
        )
#        t1 = time.time()
        tx_data = tx_data[["signer_account_id", "receiver_account_id", "converted_into_receipt_id"]]
#        t2 = time.time()
        tx_df = pd.concat([tx_df, tx_data])
#        t3 = time.time()
        tx_df = tx_df.drop_duplicates()
#        t4 = time.time()
#        print("tx_df_ size : " + str(sys.getsizeof(tx_df)/1024/1024/1024))
#        print("t1 - t0 : " +str(t1-t0))
#        print("t2 - t1 : " + str(t2 - t1))
#        print("t3 - t2 : " + str(t3 - t2))
#        print("t4 - t3 : " + str(t4 - t3))

    ara_df = pd.DataFrame()
    for ara_b in a_blobs:
        print(ara_b)
        t0 = time.time()
        ara_data = get_dataframe_from_blob(
            "actions",
            bucket.name,
            ara_b,
            token_json_path
        )
#        t1 = time.time()
        ara_data = ara_data[ara_data["action_kind"] == "FUNCTION_CALL"]
#        t2 = time.time()
        ara_data = ara_data[["receipt_id"]]
#        t3 = time.time()
        ara_df = pd.concat([ara_df, ara_data])
#        t4 = time.time()
        ara_df = ara_df.drop_duplicates()
#        t5 = time.time()
#        print("ara_df_ size : " + str(sys.getsizeof(ara_df)/1024/1024/1024))
#        print("t1 - t0 : " +str(t1-t0))
#        print("t2 - t1 : " + str(t2 - t1))
#        print("t3 - t2 : " + str(t3 - t2))
#        print("t4 - t3 : " + str(t4 - t3))
#        print("t5 - t4 : " + str(t5 - t4))

    print("creating join")
    j = tx_df.set_index("converted_into_receipt_id").join(
        ara_df.set_index("receipt_id"), how="inner"
    )[["signer_account_id", "receiver_account_id"]].reset_index(drop=True).drop_duplicates()

    a = j.groupby(by="receiver_account_id").count().reset_index()

    print(a.head())

    project_name = 'web3advertisement'
    dataset_name = 'bot_data'
    table_name = 'project_users'
    credentials = service_account.Credentials.from_service_account_file('../web3advertisement-94ba21675884.json')

    a.to_gbq(destination_table='{}.{}'.format(dataset_name, table_name), project_id=project_name, if_exists='replace', credentials=credentials)

    print("joined")
    print("j size : " + str(sys.getsizeof(j) / 1024 / 1024 / 1024))

    del tx_df
    del ara_df

    def calculateAB(f):
        if (len(f["receiver_account_id"]) == 1):
            return
      #  t0 = time.time()
      #  sm = f["signer_account_id"].iloc[0]
      #  t1 = time.time()
        l = f["receiver_account_id"].to_list()
      #  t2 = time.time()
        res = list(combinations(l, 2))
        res = [list(x) for x in res]
      #  t3 = time.time()
        #    [x.append(1) for x in res]
      #  t4 = time.time()
        #    writer.writerows(res)
      #  t5 = time.time()

        #    print(sm)
        #    print(len(f["receiver_account_id"]))
        #    print(res)
        #    print()
        #    print("l : " + str(len(l)))
        #    print("t1 : t0 : " + str(t1-t0))
        #    print("t2 : t1 : " + str(t2-t1))
        #    print("t3 : t2 : " + str(t3-t2))
        #    print("t4 : t3 : " + str(t4-t3))
        #    print("t5 : t4 : " + str(t5-t4))
        #    print()
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

#    j.to_csv("data_30_days.csv")

    print("creating result")
    j = j.groupby(level=[0,1]).count().reset_index()
    print("j size : " + str(sys.getsizeof(j) / 1024 / 1024 / 1024))


#    j.to_csv("result.csv")

    project_name = 'web3advertisement'
    dataset_name = 'bot_data'
    table_name = 'project_users_intersection'

    j.to_gbq(destination_table='{}.{}'.format(dataset_name, table_name), project_id=project_name, if_exists='replace', credentials=credentials)



