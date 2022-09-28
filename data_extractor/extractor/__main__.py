import getopt
import sys
from data_extractor.extractor import config as c, data_extractor as de
from data_extractor.cloud_storage_utils import google_cloud_storage_utils as gcsu
import datetime

if __name__ == '__main__':
    argv = sys.argv[1:]
    options = "atn:ls:r:b:"
    long_options = ["actions", "transactions", "network=", "local", "start_date=", "date_range=", "bucket="]

    actions = c.GET_ACTIONS_DATA_DEFAULT
    transactions = c.GET_TRANSACTIONS_DATA_DEFAULT
    network = c.DEFAULT_NETWORK
    token_json_path = c.TOKEN_JSON_PATH
    start_date = datetime.date.today() - datetime.timedelta(days=1)
    dates_range = c.DEFAULT_DATE_RANGE
    bucket = c.DEFAULT_BUCKET_NAME

    try:
        opts, args = getopt.getopt(argv, options, long_options)

        for opt, value in opts:
            if opt in ("-a", "--actions"):
                actions = True

            elif opt in ("-t", "--transactions"):
                transactions = True

            elif opt in ("-n", "--network"):
                network = value

            elif opt in ("-l", "--local"):
                token_json_path = c.LOCAL_TOKEN_JSON_PATH

            elif opt in ("-s", "--start_date"):
                try:
                    start_date = datetime.datetime.strptime(value, "%d%m%Y")
                except ValueError as e:
                    print("ERROR OCCURED: --start_date must be in %d%m%Y format, but " + value + " was given")
                    sys.exit(1)

            elif opt in ("-r", "--date_range"):
                try:
                    dates_range = int(value)
                except ValueError as e:
                    print("ERROR OCCURED: --date-range must be integer, but " + value + " was given")
                    sys.exit(1)

            elif opt in ("-b", "--bucket"):
                bucket = value

    except getopt.GetoptError as e:
        print('Error while parsing command line arguments : ' + str(e))

    creds = c.INDEXER_CREDENTIALS[network]

    print("get transactions data : " + str(actions))
    print("get actions data : " + str(transactions))
    print("token json : " + str(token_json_path))
    print("connecting to " + str(network) + ", host : " + creds["host"] + ", database : " + creds["database"])
    print("start date : " + str(start_date))
    print("date range : " + str(dates_range))

    if (transactions == True):
        print("getting transactions")
        de.extract_data(
            query = c.QUERY_TRANSACTIONS,
            network_creds = creds,
            start_time = start_date,
            dates_range = dates_range,
            bucket = gcsu.get_bucket(token_json_path, bucket),
            blob_path = c.BLOB_NAMES[network]["transactions"]
        )

    if (actions == True):
        de.extract_data(
            query = c.QUERY_ACTIONS,
            network_creds = creds,
            start_time = start_date,
            dates_range = dates_range,
            bucket = gcsu.get_bucket(token_json_path, bucket),
            blob_path = c.BLOB_NAMES[network]["actions"]
        )
