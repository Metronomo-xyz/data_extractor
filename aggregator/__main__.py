import getopt
import sys
from aggregator import config as c
from aggregator import aggregator
from google.cloud import storage
#import datetime
from datetime import datetime, timedelta

if __name__ == '__main__':
    argv = sys.argv[1:]
    options = "atln:b:s:r:"
    long_options = ["actions", "transactions", "local", "network=", "bucket=", "start-date=", "range-dates="]

    entities = list()
    years_list = c.DEFAULT_YEARS
    months_list = c.DEFAULT_MONTHS
    network = c.DEFAULT_NETWORK
    token_json_path = c.TOKEN_JSON_PATH
    bucket_name = c.DEFAULT_BUCKET_NAME
    start_date = datetime.today() - timedelta(days=1)
    dates_range = 1

    try:
        opts, args = getopt.getopt(argv, options, long_options)

        for opt, value in opts:
            if opt in ("-a", "--actions"):
                entities.append("actions")

            elif opt in ("-t", "--transactions"):
                entities.append("transactions")

            elif opt in ("-l", "--local"):
                token_json_path = c.LOCAL_TOKEN_JSON_PATH

            elif opt in ("-n", "--network"):
                network = value

            elif opt in ("-b", "--bucket"):
                bucket_name = value

            elif opt in ("-s", "--start-date"):
                start_date = value

            elif opt in ("-r", "--range-dates"):
                dates_range = int(value)

    except getopt.GetoptError as e:
        print('Error while parsing command line arguments : ' + str(e))

    print("args handled")

    dates = [datetime.strftime(start_date - timedelta(days=x), '%Y-%m-%d') for x in range(dates_range)]
    print("dates : " + str(dates))

    storage_client = storage.Client.from_service_account_json(token_json_path)
    bucket = storage_client.bucket(bucket_name)

    aggregator.aggregate_daily(storage_client, bucket, network, dates, token_json_path)
