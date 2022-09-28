DEFAULT_NETWORK = "mainnet"
GET_ACTIONS_DATA_DEFAULT = False
GET_TRANSACTIONS_DATA_DEFAULT = False

INDEXER_CREDENTIALS = {
    "mainnet": {
        "host" : "mainnet.db.explorer.indexer.near.dev",
        "database" : "mainnet_explorer",
        "user" : "public_readonly",
        "password": "nearprotocol"
    },
    "testnet" : {
        "host" : "testnet.db.explorer.indexer.near.dev",
        "database" : "testnet_explorer",
        "user" : "public_readonly",
        "password": "nearprotocol"
    }
}

TOKEN_JSON_PATH = 'web3advertisement-b54340ad58ad.json'

LOCAL_TOKEN_JSON_PATH = 'C:/Users/yaroslav/Documents/Web3MarketingPlatform/CodeTools/web3advertisement-b54340ad58ad.json'

DEFAULT_DATE_RANGE = 1

QUERY_TRANSACTIONS = '''
    select 
        *
    from transactions
    where
        block_timestamp >= @start_time
        and block_timestamp < @end_time
    '''

QUERY_ACTIONS = '''
    select *
    from action_receipt_actions
    where 
        receipt_included_in_block_timestamp >= @start_time
        and receipt_included_in_block_timestamp < @end_time
    '''

DEFAULT_BUCKET_NAME = "near-data"

BLOB_NAMES = {
    "mainnet": {
        "transactions" : "mainnet/hourly_data/transactions/",
        "actions" : "mainnet/hourly_data/action_receipt_actions/"
    },
    "testnet" : {
        "transactions" : "testnet/hourly_data/transactions/",
        "actions" : "testnet/hourly_data/action_receipt_actions/"
    }
}