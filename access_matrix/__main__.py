from access_matrix import config as c
import cloud_storage_utils as csu
from access_matrix import access_matrix as am
import sys
import getopt


if __name__ == '__main__':
    argv = sys.argv[1:]
    options = "ln:b:y:m:"
    long_options = ["local", "network=", "bucket=", "years=", "months="]

    years_list = c.DEFAULT_YEARS
    months_list = c.DEFAULT_MONTHS
    network = c.DEFAULT_NETWORK
    token_json_path = c.TOKEN_JSON_PATH
    bucket_name = c.DEFAULT_BUCKET_NAME

    try:
        opts, args = getopt.getopt(argv, options, long_options)

        for opt, value in opts:
            if opt in ("-l", "--local"):
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

    bucket = csu.get_bucket(token_json_path, bucket_name)
    access_data_blob_name = "gs://" + c.DEFAULT_BUCKET_NAME + "/" + c.DEFAULT_ACCESS_TRANSACTIONS_BLOB_NAME
    access_transactions = am.get_access_transactions(network, years_list, months_list, bucket, token_json_path)
    csu.write_dataframe_to_blob(access_transactions, bucket, access_data_blob_name)

    access_matrix = get_access_matrix(access_transactions)




