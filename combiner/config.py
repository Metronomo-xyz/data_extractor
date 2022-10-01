DEFAULT_MONTHS = list(["08"])
DEFAULT_YEARS = list(["2022"])
DEFAULT_NETWORK = "mainnet"

TOKEN_JSON_PATH = '../web3advertisement-b54340ad58ad.json'
LOCAL_TOKEN_JSON_PATH = 'C:/Users/yaroslav/Documents/Web3MarketingPlatform/CodeTools/web3advertisement-b54340ad58ad.json'

ENTITIES = {
    "transactions" : {
        "fields": ["signer_account_id", "receiver_account_id", "converted_into_receipt_id", "block_timestamp"],
        "files_part" : "transactions"
    },
    "actions" : {
        "fields": ["receipt_id", "action_kind", "args"],
        "files_part" : "actionreceiptactions"
    }
}

BLOB_PATHS = {
    "mainnet": {
        "hourly": {
            "transactions" : "mainnet/hourly_data/transactions/",
            "actions" : "mainnet/hourly_data/action_receipt_actions/"
        },
        "monthly" : {
            "transactions" : "mainnet/monthly_data/transactions/",
            "actions" : "mainnet/monthly_data/action_receipt_actions/"
        }
    },
    "testnet" : {
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

DEFAULT_BUCKET_NAME = "near-data"