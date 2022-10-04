from projects_similarity import config as c
from projects_similarity import projects_similarity as ps
import google_cloud_storage_utils as csu
import sys
import getopt
import pandas as pd


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

    bucket = csu.get_bucket(token_json_path, bucket_name)
    access_matrix_blob_path = c.ACCESS_MATRIX_BLOB_PATH + "/"

    blobs = csu.get_blob_list(token_json_path, bucket)
    blobs = list(filter(lambda b: c.ACCESS_MATRIX_BLOB_PATH in str(blob),blobs))

    vectors = pd.DataFrame()

    for blob in blobs:
        print(blob)
        blob_path = "gs://" + bucket.name + "/" + blob
        d = pd.read_csv(blob, storage_options={"token": token_json_path})
        vectors = pd.concat([vectors, d])

    vectors["has_transactions"] = vectors["num_data"].fillna(0).astype(int)

    common_users_matrix = ps.get_common_users_matrix(vectors)
    cosine_matrix = ps.get_cosine_matrix(vectors)