import getopt
import sys
from combiner import config as c
from combiner import combiner
from google.cloud import storage

if __name__ == '__main__':
    argv = sys.argv[1:]
    options = "atln:b:y:m:"
    long_options = ["actions", "transactions", "local", "network=", "bucket=", "years=", "months="]

    entities = list()
    years_list = c.DEFAULT_YEARS
    months_list = c.DEFAULT_MONTHS
    network = c.DEFAULT_NETWORK
    token_json_path = c.TOKEN_JSON_PATH
    bucket_name = c.DEFAULT_BUCKET_NAME

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

            elif opt in ("-y", "--years"):
                years_list = list(value.split(" "))

            elif opt in ("-m", "--months"):
                months_list = list(value.split(" "))

    except getopt.GetoptError as e:
        print('Error while parsing command line arguments : ' + str(e))

    print("args handled")

#(entities, network, years_list, months_list, bucket, token_json_path, storage_client):

    storage_client = storage.Client.from_service_account_json(token_json_path)
    bucket = storage_client.bucket(bucket_name)

    combiner.combine_data(
        entities,
        network,
        years_list,
        months_list,
        bucket,
        token_json_path,
        storage_client
    )