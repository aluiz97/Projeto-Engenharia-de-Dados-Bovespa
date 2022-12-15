from datetime import date
from bovespa_writer import BovespaDataWriter
from bovespa_s3 import bucket, create_folders
import time

stocks = ['PETR4', 'BBAS3', 'VALE3', 'EGIE3', 'ITSA4']
bucket_name = 'bovespa-etl-project'
bucket_adress = f's3://{bucket_name}'

initial_date = date(2022, 10, 1)
final_date = date(date.today().year, date.today().month, date.today().day)

def etl_bucket():

    bucket(bucket_name)
    time.sleep(2)


def etl_folders():

    create_folders(bucket_name, stocks)
    time.sleep(2)

def etl_function():

    BovespaDataWriter(initial_date, final_date)._write_datas(stocks, bucket_name)
    