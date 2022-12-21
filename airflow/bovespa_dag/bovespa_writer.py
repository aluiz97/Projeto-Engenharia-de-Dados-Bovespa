import bovespa_api
import json
import os
from datetime import date
from abc import ABC, abstractmethod
from bovespa_s3 import upload_file
from s3fs import S3FileSystem
from dotenv import load_dotenv
from os import getenv

load_dotenv('/home/ubuntu/.env')

class DataWriter(ABC):

    @abstractmethod
    def __init__(self, **kwargs) -> None:
        pass

    @abstractmethod
    def _write(self, **kwargs) -> None:
        pass


class BovespaDataWriter(DataWriter):

    def __init__(self,  initial_date: date, final_date: date) -> None:
        self.initial_date = initial_date
        self.final_date = final_date

    def _write(self, stock: str, adress: str) -> None:

        s3 = S3FileSystem(anon=False, key=getenv('AWS_ID'), secret=getenv('AWS_KEY'))

        acao = bovespa_api.Acoes(stock).get_data(self.initial_date, self.final_date)

        for i in range(len(acao)):
            
            with s3.open(f'{adress}/{stock}-{acao[i]["DATPRG"][0:10]}', 'w') as fp:
                json.dump(acao[i], fp)

    def _write_datas(self, stocks: list, bucket_adress: str) -> None:

        for i in range(len(stocks)):

            adress = f'{bucket_adress}/{stocks[i]}'
            self._write(stocks[i], adress)
