import bovespa_api
import json
import os
from datetime import date
from abc import ABC, abstractmethod
from s3fs import S3FileSystem

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

        s3 = S3FileSystem()

        acao = bovespa_api.Acoes(stock).get_data(self.initial_date, self.final_date)
        stock_name = stock

        for i in range(len(acao)):

            with s3.open(f'{adress}/{stock_name}-{acao[i]["DATPRG"][0:10]}', 'w') as fp:
                json.dump(acao[i], fp)

    def _write_datas(self, stocks: list, bucket_adress: str) -> None:

        for i in range(len(stocks)):

            adress = f'{bucket_adress}/{stocks[i]}'

            self._write(stocks[i], adress)
