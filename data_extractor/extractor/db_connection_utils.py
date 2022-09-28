import psycopg2
import time


def get_connection(network_creds, sleep_time=5):
    flag = 0
    while flag == 0:
        try:
            db = psycopg2.connect(host=network_creds.get("host"), database=network_creds.get("database"),
                                  user=network_creds.get("user"), password=network_creds.get("password"))
            return db
        except Exception as ex:
            print("Oops!", ex.__class__, "occurred. trying to reconnect")
            time.sleep(sleep_time)