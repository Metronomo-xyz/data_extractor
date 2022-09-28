import getopt
import sys
from data_combiner import config as c

if __name__ == '__main__':
    argv = sys.argv[1:]
    options = "atlb:y:m:"
    long_options = ["actions", "transactions", "local", "bucket=", "years=", "months="]

    entities = list()
    years_list = c.DEFAULT_YEARS
    months_list = c.DEFAULT_MONTHS
    actions = c.COMBINE_ACTIONS_DATA_DEFAULT
    transactions = c.COMBINE_TRANSACTIONS_DATA_DEFAULT
    token_json_path = c.TOKEN_JSON_PATH
    bucket = c.DEFAULT_BUCKET_NAME

    try:
        opts, args = getopt.getopt(argv, options, long_options)

        print(opts)
        print(args)

        for opt, value in opts:
            if opt in ("-a", "--actions"):
                entities.append("actions")

            elif opt in ("-t", "--transactions"):
                entities.append("transactions")

            elif opt in ("-l", "--local"):
                token_json_path = c.LOCAL_TOKEN_JSON_PATH

            elif opt in ("-b", "--bucket"):
                bucket = value

            elif opt in ("-y", "--years"):
                bucket = value

            elif opt in ("-m", "--months"):
                bucket = value

    except getopt.GetoptError as e:
        print('Error while parsing command line arguments : ' + str(e))

    print("args handled")

    sys.exit(0)

    for entity in entities:
        for year in years_list:
            for month in months_list:
                data = pd.DataFrame()
                print("entity : " + str(entity) + " : year : " + str(year) + " : month : " + str(month))
                blobs = list(filter(lambda f: ((f.split("-")[1] == month) & (f.split("-")[0].split("mainnet"+entities_files_parts[entity])[1] == str(year))), filter(lambda b: ('/'+entity+'/' in b) & ("mainnet/" in b), all_blobs)))
                for b in blobs:
                    print(b)
                    d = pd.read_csv("gs://near-data/" + b, storage_options={"token": token_json_path})[eintities_fields[entity]]
                    data = pd.concat([data, d])

                new_blob_name = "monthly_data/"+str(entity)+"/"+str(year)+"/"+str(month)+".csv"
                root_bucket.blob(new_blob_name).upload_from_string(data.to_csv(), 'text/csv')