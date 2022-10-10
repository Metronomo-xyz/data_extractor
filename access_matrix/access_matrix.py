import json
from access_matrix import config as c
import pandas as pd
import google_cloud_storage_utils as csu
import sys


def getArgsJSON(s):
    try:
        js = json.loads(s.replace(" None", " \'None\'").replace("\'", "\""))
        return js
    except Exception as e:
        print(e)


def get_entites_data(entity, network, years_list, months_list, bucket, token_json_path):
    data = pd.DataFrame()
    for year in years_list:
        print("year : " + str(year))
        for month in months_list:
            print("month : " + str(month))
            bucket_name = bucket.name
            blob_name = "gs://" + bucket_name + "/" + c.BLOB_PATHS[network]["monthly"][entity] + year + "/" + month + ".csv"
            print(blob_name)
            try:
                d = pd.read_csv(blob_name, storage_options={"token": token_json_path})[c.ENTITIES[entity]["fields"]]
                print(d.head())
            except KeyError as e:
                print("Nessesary fields not found in data")
                print("Key Error : " + str(e))
                sys.exit(1)

            if (entity == "action"):
                d = d[d.args.apply(lambda x: "\'permission_kind\': \'FUNCTION_CALL\'" in x)]
                d["access_receiver"] = d.args.apply(lambda x: getArgsJSON(x)["access_key"]["permission"]["permission_details"]["receiver_id"])
                d = d[["receipt_id", "access_receiver"]]

            data = pd.concat([data,d])
    return data


def get_access_transactions(network, years_list, months_list, bucket, token_json_path):
    print("getting transactions")
    transactions = get_entites_data("transactions", network, years_list, months_list, bucket, token_json_path)

    print("getting actions")
    actions = get_entites_data("actions", network, years_list, months_list, bucket, token_json_path)

    print("joining")
    joined_data = transactions.set_index(["converted_into_receipt_id"]).join(actions.set_index(["receipt_id"]), how="inner")

    print("transforming")
    joined_data = joined_data[["signer_account_id", "access_receiver", "block_timestamp"]]

    print("saving basis")
    joined_data.reset_index()[["signer_account_id", "access_receiver", "block_timestamp"]].drop_duplicates()\

    return joined_data

def get_access_matrix(access_transactions, network, bucket):
    access_transactions = access_transactions[["signer_account_id", "access_receiver"]].drop_duplicates()
    access_transactions["num"] = 1
    access_transactions = access_transactions.set_index(["signer_account_id", "access_receiver"])

    projects = pd.DataFrame(access_transactions.reset_index()[access_transactions.reset_index().signer_account_id != access_transactions.reset_index().access_receiver].access_receiver.drop_duplicates().reset_index(drop=True))
    projects["key"] = 0

    signers = pd.DataFrame(access_transactions.reset_index()[access_transactions.reset_index().signer_account_id != access_transactions.reset_index().access_receiver].signer_account_id.drop_duplicates().reset_index(drop=True))
    signers["key"] = 0

    access_matrix = projects.set_index("key").join(signers.set_index("key"), lsuffix="_project", rsuffix="_signer",how="outer")
    access_matrix["num"] = 0
    access_matrix = access_matrix.set_index(["signer_account_id", "access_receiver"])

    for project in projects["access_receiver"]:
        print(str(project))
        access_matrix_part = access_matrix[access_matrix.index.get_level_values('access_receiver').isin([project])]
        access_matrix_part = access_matrix_part.join(access_transactions, how='left', lsuffix="_fj",rsuffix="_data")
        access_matrix_part["num_data"] = access_matrix_part["num_data"].fillna(0)
        access_matrix_part = access_matrix_part["num_data"]
        access_matrix_part.columns = ["num"]

        #TODO: FIX blob name
        blob_name = "gs://" + bucket.name() + "/" + c.ACCESS_MATRIX_BLOB_PATH + "/" + str(project) + ".csv"
        csu.write_dataframe_to_blob(access_matrix_part, bucket, blob_name)
