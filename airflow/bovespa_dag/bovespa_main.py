from datetime import date
from bovespa_writer import BovespaDataWriter

stocks = ['PETR4', 'BBAS3', 'VALE3', 'EGIE3']
bucket_adress = 's3://bovespa-etl-project'

initial_date = date(2022, 10, 1)
final_date = date(date.today().year, date.today().month, date.today().day)

def etl_function():

    BovespaDataWriter(initial_date, final_date)._write_datas(stocks, bucket_adress)
    