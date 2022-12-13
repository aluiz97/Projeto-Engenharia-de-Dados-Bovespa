from datetime import date
from abc import ABC, abstractmethod

import requests
import logging

# %%
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# %%
class BaseApi(ABC):

    def __init__(self, acao:str) -> None:
        self.acao = acao
        self.endpoint = "https://www.okanebox.com.br/api"

    @abstractmethod
    def _get_endpoint(self, **kwargs) -> str:
        pass

    @abstractmethod
    def get_data(self, **kwargs) -> dict:
       pass

# %%
class Acoes(BaseApi):

    api_endpoint = "acoes/hist"

    def _get_endpoint(self, initial_date: date, final_date: date) -> str:

        initial_date_str = f"{initial_date.year}{initial_date.month}{initial_date.day}"
        final_date_str = f"{final_date.year}{final_date.month}{final_date.day}"

        return f"{self.endpoint}/{self.api_endpoint}/{self.acao}/{initial_date_str}/{final_date_str}"

    def get_data(self, initial_date: date, final_date: date) -> dict:

        endpoint = self._get_endpoint(initial_date, final_date)
        logger.info(f"Getting data from endpoint: {endpoint}")
        response = requests.get(endpoint)
        response.raise_for_status()
        return response.json()
