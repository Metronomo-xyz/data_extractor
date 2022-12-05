from abc import ABC, abstractmethod
import pandas as pd
from google_cloud_storage_utils import google_cloud_storage_utils as csu
from google.cloud import storage
import requests
import json
import time




class DataConnector(ABC):
    @abstractmethod
    def getData(self) -> pd.DataFrame:
        pass

class MetronomoTXCloudStorageConnector(DataConnector):
    # This class implements TXDataConnector class
    # and incapsulates the logic to retrieve data from
    # Metronomno.xyz Google Cloud Storage
    # Current implementation will NOT be useful for anyone who wants to use power_users tool
    # However if you use data from your own Google Cloud Storage
    # it is possible to adjust the code to your need

    # The main source of data below was NEAR Indexer for Explorer PostgresSQL database
    # https://github.com/near/near-indexer-for-explorer
    # We get transactions table once a day for previous day in a hourly batches

    ENTITIES = {
        "transactions": {
            "fields": ["signer_account_id", "receiver_account_id", "converted_into_receipt_id"],
            "fields_extended": ["signer_account_id", "receiver_account_id", "converted_into_receipt_id"],
            "files_part": "transactions"
        },
        "actions": {
            "fields": ["receipt_id", "action_kind"],
            "fields_extended": ["receipt_id", "action_kind", "args"],
            "files_part": "actionreceiptactions"
        }
    }

    BLOB_PATHS = {
        "mainnet": {
            "hourly": {
                "transactions": "mainnet/hourly_data/transactions/",
                "actions": "mainnet/hourly_data/action_receipt_actions/"
            },
            "monthly": {
                "transactions": "mainnet/monthly_data/transactions/",
                "actions": "mainnet/monthly_data/action_receipt_actions/"
            },
            "daily": {
                "transactions": "mainnet/daily_data/transactions/",
                "actions": "mainnet/daily_data/action_receipt_actions/"
            },
            "daily_extended": {
                "transactions": "mainnet/daily_data_extended/transactions/",
                "actions": "mainnet/daily_data_extended/action_receipt_actions/"
            }
        },
        "testnet": {
            "hourly": {
                "transactions": "testnet/hourly_data/transactions/",
                "actions": "testnet/hourly_data/action_receipt_actions/"
            },
            "monthly": {
                "transactions": "testnet/monthly_data/transactions/",
                "actions": "testnet/monthly_data/action_receipt_actions/"
            }
        }
    }

    def __init__(self, bucket_name, token_json_path, network, dates, granularity):
        self.bucket_name = bucket_name
        self.token_json_path = token_json_path
        self.storage_client = storage.Client.from_service_account_json(self.token_json_path)
        self.bucket = self.storage_client.bucket(self.bucket_name)
        self.network = network
        self.dates = dates
        self.granularity = granularity

    def __str__(self):
        return "CloudStorageConnectror object : " + "\n" + \
               "bucket_name : " + str(self.bucket_name) + "\n" + \
               "token_json_path : " + str(self.token_json_path) + "\n" + \
               "storage_client : " + str(self.storage_client) + "\n" + \
               "bucket : " + str(self.bucket) + "\n" + \
               "network : " + str(self.network) + "\n" + \
               "dates : " + str(self.dates) + "\n" + \
               "granularity : " + str(self.granularity)

    def getData(self):
        all_blobs = csu.get_blob_list(self.storage_client, self.bucket)
        tx_blobs = csu.filter_blobs_by_path(
            all_blobs,
            self.BLOB_PATHS[self.network][self.granularity]["transactions"],
        )
        tx_blobs = csu.filter_blobs_by_dates(tx_blobs, self.dates)
        print("tx_blobs : ")
        print(tx_blobs)

        tx_df = pd.DataFrame()
        for tx_b in tx_blobs:
            print("current blob : ")
            print(tx_b)
            tx_data = csu.get_dataframe_from_blob(
                self.bucket,
                tx_b,
                self.ENTITIES["transactions"]["fields"],
                self.token_json_path
            )
            tx_data = tx_data[["signer_account_id", "receiver_account_id", "converted_into_receipt_id"]]
            tx_df = pd.concat([tx_df, tx_data])
        print("tx_df head : ")
        print(tx_df.head(5))
        return tx_df

class MintbaseNFTActivitiesConnector(DataConnector):
    def getData(self, recipies):
        recepies_string = str(recipies).replace("\'", "\"")
        url = "https://interop-mainnet.hasura.app/v1/graphql"

        headers = {
            'Content-Type': 'application/json',
        }

        payload = json.dumps({
            "query": "query MyQuery {nft_activities(where: {receipt_id: {_in: " + recepies_string + "}}){receipt_id,price,kind,token_id,nft_contract_id,tx_sender,action_receiver,action_sender,timestamp,memo}}"
        })

        response = requests.request("POST", url, headers=headers, data=payload)

        return pd.DataFrame(response.json()["data"]["nft_activities"])
