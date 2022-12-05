from power_users import config as c
from power_users import data_connectors as dc
from power_users import power_users as pu
import getopt
import sys
import datetime


if __name__ == '__main__':
    argv = sys.argv[1:]
    options = "ln:b:s:r:"
    long_options = ["local", "network=", "bucket=", "start_date=", "date_range="]

    entities = list()
    network = c.DEFAULT_NETWORK
    token_json_path = c.TOKEN_JSON_PATH
    bucket_name = c.DEFAULT_BUCKET_NAME
    start_date = datetime.date.today() - datetime.timedelta(days=1)
    dates_range = 1

    try:
        opts, args = getopt.getopt(argv, options, long_options)

        for opt, value in opts:
            if opt in ("-l", "--local"):
                token_json_path = c.LOCAL_TOKEN_JSON_PATH

            elif opt in ("-n", "--network"):
                network = value

            elif opt in ("-b", "--bucket"):
                bucket_name = value

            elif opt in ("-s", "--start_date"):
                try:
                    start_date = datetime.datetime.strptime(value, "%d%m%Y").date()
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

    dates = [start_date - datetime.timedelta(days=x) for x in range(dates_range)]

    print("start")

    gcs_connector = dc.MetronomoTXCloudStorageConnector(bucket_name, c.LOCAL_TOKEN_JSON_PATH, network, dates, "daily")
    print(gcs_connector)
    df = gcs_connector.getData()

    recepies = df["converted_into_receipt_id"].tolist()
    print("recepies len : ")
    print(len(recepies))

    mintbaseNFTActivityConnector = dc.MintbaseNFTActivitiesConnector()
    print(mintbaseNFTActivityConnector)
    data = mintbaseNFTActivityConnector.getData(recepies)

    print(data[data["kind"] == "make_offer"])

    result = pu.getPowerUsers(data, 'graffbase.mintbase1.near')
    print(result)
