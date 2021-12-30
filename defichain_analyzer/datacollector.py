import configparser
import logging
from dataclasses import dataclass
from datetime import date

import pandas as pd
import requests

from helpers import execute_transformation

TODAY = date.today()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(threadName)s %(name)s %(message)s",
)


@dataclass
class Datacollector:
    """Data class to load and write data for the Defichain DEX"""

    def __post_init__(self) -> None:
        """Additional initialization without overriding dataclass init"""
        self.config = configparser.ConfigParser()
        self.config.read("config.ini")

    @property
    def tokens(self) -> pd.DataFrame:
        """All available tokens on Defichain DEX at time of execution"""
        token_endpoint = self.config["API"]["token_endpoint"]
        r = requests.get(token_endpoint)
        ret_list = execute_transformation(r)
        ret_cols = [
            "id",
            "symbol",
            "name",
            "mintable",
            "tradeable",
            "isDAT",
            "isLPS",
            "isLoanToken",
            "minted",
        ]
        return pd.DataFrame(ret_list)[ret_cols]

    @property
    def pool_pairs(self) -> pd.DataFrame:
        """All pool pairs on Defichain DEX at time of execution"""
        pool_pair_endpoint = self.config["API"]["poolpair_endpoint"]
        r = requests.get(pool_pair_endpoint)
        ret_list = execute_transformation(r)
        return pd.DataFrame(ret_list)

    @property
    def yieldfarming(self) -> pd.DataFrame:
        """All yield farming pairs on Defichain DEX at time of execution"""
        poolpair_endpoint = self.config["API"]["poolpair_endpoint"]
        r = requests.get(poolpair_endpoint)
        ret_list = execute_transformation(r)
        return pd.DataFrame(ret_list)

    def persist_csv_to_s3(self, bucket: str = "defichain-analyzer") -> None:
        """Write all dataframe attributes to AWS s3 """
        from io import StringIO
        import boto3

        s3_resource = boto3.resource("s3")
        year, month, day = TODAY.year, TODAY.month, TODAY.day
        date_path = f"{year}/{month}/{day}"

        dfs = [self.tokens, self.pool_pairs, self.yieldfarming]
        df_names = ["tokens", "pool_pairs", "yield_farming"]

        for i in range(len(dfs)):
            csv_buffer = StringIO()
            dfs[i].to_csv(csv_buffer)

            prefix = f"{df_names[i]}/{date_path}/"
            file_name = f"{prefix}{df_names[i]}_{str(TODAY).replace('-', '_')}.csv"
            s3_resource.Object(bucket, file_name).put(Body=csv_buffer.getvalue())
            logging.info(f"{file_name} was successfully uploaded")
