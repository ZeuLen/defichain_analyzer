from dataclasses import dataclass
import configparser

import os
import requests
from datetime import date
import pandas as pd

from utils import json_to_df

import logging
logging.basicConfig(encoding='utf-8', level=logging.DEBUG)


@dataclass
class Defichain:
    """Class to bundle functionality to analyze DFI token"""

    def __post_init__(self):
        """Additional initialization without overriding dataclass init"""

        self.init_config_parser()

    def init_config_parser(self):
        self.config = configparser.ConfigParser()
        self.config.read('config.ini')
        logging.info(f"{self.config.sections()}")

    @property
    def tokens(self):
        token_endpoint = self.config["API"]["token_endpoint"]

        r = requests.get(token_endpoint)

        ret_list = []
        if self.request_status_check(r):
            data = r.json()
            ret_list = json_to_df(data, ret_list)
        ret_cols = ["id", "symbol", "name", "mintable", "tradeable", "isDAT", "isLPS", "isLoanToken", "minted"]
        return pd.DataFrame(ret_list)[ret_cols]

    @property
    def pool_pairs(self):
        poolpair_endpoint = self.config["API"]["poolpair_endpoint"]
        r = requests.get(poolpair_endpoint)

        ret_list = []
        if self.request_status_check(r):
            data = r.json()
            ret_list = json_to_df(data, ret_list)
        return pd.DataFrame(ret_list)

    @property
    def yieldfarming(self):
        poolpair_endpoint = self.config["API"]["poolpair_endpoint"]
        r = requests.get(poolpair_endpoint)

        ret_list = []
        if self.request_status_check(r):
            data = r.json()
            ret_list = json_to_df(data, ret_list)
        return pd.DataFrame(ret_list)

    def write_data(self, s3=False):
        today = date.today()

        # write token data - convert to parquet once finished
        #self.tokens.to_parquet(f"../data/tokens_{today}.parquet.gzip", compression='gzip')

        if not os.path.isdir('temp'):
            os.mkdir("temp")

        self.tokens.to_csv(f"temp/tokens_{today}.csv", index=False)
        self.pool_pairs.to_csv(f"temp/pool_pairs_{today}.csv", index=False)
        self.yieldfarming.to_csv(f"temp/yield_farming_{today}.csv", index=False)
        pass

    def request_status_check(self, r):
        if r.status_code != 200:
            raise ValueError("API Request was not successful")
        else:
            return True



if __name__ == '__main__':
    logging.info("Script beginning to run...")
    defi_chain = Defichain()
    defi_chain.write_data()
