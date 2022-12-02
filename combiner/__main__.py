import getopt
import sys
from combiner import config as c
from combiner import combiner
from google.cloud import storage
import datetime

if __name__ == '__main__':
    argv = sys.argv[1:]
    options = "atln:b:s:r:"
    long_options = ["actions", "transactions", "local", "network=", "bucket=", "start_date=", "date_range="]

    entities = list()
    years_list = c.DEFAULT_YEARS
    months_list = c.DEFAULT_MONTHS
    network = c.DEFAULT_NETWORK
    token_json_path = c.TOKEN_JSON_PATH
    bucket_name = c.DEFAULT_BUCKET_NAME
    start_date = datetime.date.today() - datetime.timedelta(days=1)
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

            elif opt in ("-s", "--start_date"):
                try:
                    start_date = datetime.date.strptime(value, "%d%m%Y")
                except ValueError as e:
                    print("ERROR OCCURED: --start_date must be in %d%m%Y format, but " + value + " was given")
                    sys.exit(1)

            elif opt in ("-r", "--date_range"):
                try:
                    dates_range = int(value)
                except ValueError as e:
                    print("ERROR OCCURED: --date-range must be integer, but " + value + " was given")
                    sys.exit(1)

    except getopt.GetoptError as e:
        print('Error while parsing command line arguments : ' + str(e))

    print("args handled")

    storage_client = storage.Client.from_service_account_json(token_json_path)
    bucket = storage_client.bucket(bucket_name)


    dates = [start_date - datetime.timedelta(days=x) for x in range(dates_range)]
    print("dates : " + str(dates))

    combiner.combine_data(
        entities,
        network,
        dates,
        bucket,
        token_json_path,
        storage_client
    )