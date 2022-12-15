import bovespa_api
import json
import os
from datetime import date
from abc import ABC, abstractmethod
from bovespa_s3 import upload_file

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

        acao = bovespa_api.Acoes(stock).get_data(self.initial_date, self.final_date)

        for i in range(len(acao)):

            #file_name = f'extracted_at={acao[i]["DATPRG"][0:10]}'
            file_to_save = json.dumps(acao[i])
            upload_file(adress, stock, stock, file_to_save)

    def _write_datas(self, stocks: list, bucket_adress: str) -> None:

        for i in range(len(stocks)):

            self._write(stocks[i], bucket_adress)
