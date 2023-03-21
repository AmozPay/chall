
from os import listdir, path
from typing import List, Dict, Set, Tuple
import json

Price = List[float]
TokenPair = str
Exchange = str
ExchangePrices = Dict[TokenPair, List[Price]]
Market = Dict[str, ExchangePrices]
UnixTimestamp = int

class ExchangePriceIngest:
    _market: Market
    _time_index: int = 0
    _token_pairs: Set[TokenPair] = set()
    _exchanges: Set[str] = set()
    def __init__(self, data_directory: str):
        self._market = self.__get_data(data_directory)

    def get_current_price(self, token: str) -> Tuple[UnixTimestamp, Dict[Exchange, Price]]:
        price_per_exchange: Dict[Exchange, Price] = {}
        timestamp: int = 0
        for exchange in self._exchanges:
            token_prices = self._market.get(exchange).get(token)
            if token_prices != None and self._time_index < len(token_prices):
                timestamp = token_prices[self._time_index].pop(0)
                price_per_exchange[exchange] = token_prices[self._time_index]
        self._time_index += 1
        return (timestamp, price_per_exchange)

    @property
    def exchanges(self):
        return self._exchanges

    @property
    def token_pairs(self):
        return self._token_pairs

    def __list_exchanges(self, data_directory: str) -> None:
        for exchange in listdir(data_directory):
            self._exchanges.add(exchange)

    def __get_exchange_prices(self, data_directory: str, exchange_dir: str) -> ExchangePrices:
        exchange_prices: ExchangePrices = {}
        exchange_full_dir = path.join(data_directory, exchange_dir)
        for token_pair_prices_file in listdir(exchange_full_dir):
            token_pair = token_pair_prices_file.split('-')[0]
            self._token_pairs.add(token_pair)
            with open(path.join(exchange_full_dir, token_pair_prices_file)) as fp:
                exchange_prices[token_pair] = json.load(fp)
                exchange_prices[token_pair].sort(key = lambda arr: arr[0])
        return exchange_prices

    def __get_data(self, data_directory: str) -> any:
        self.__list_exchanges(data_directory)
        exchange_dict: Market = {}
        for exchange in self._exchanges:
            exchange_dict[exchange] = self.__get_exchange_prices(data_directory, exchange)
        return exchange_dict
