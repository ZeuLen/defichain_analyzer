import logging
from configparser import ConfigParser
from datetime import date
from typing import List

import boto3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(threadName)s %(name)s %(message)s",
)


class BucketManager:
    """Class with functionality for managing aws s3 buckets using boto3"""

    def __init__(self, cfg: ConfigParser):
        self.client = boto3.client("s3")
        self.today = date.today()
        self.config = cfg

    def get_bucket_prefixes(self, bucket: str = "defichain-analyzer") -> List[str]:
        """ Get all existig s3 bucket prefixes"""
        objects = self.client.list_objects(Bucket=bucket)
        keys = [k["Key"] for k in objects["Contents"]]
        return keys

    def check_object_existence(self, bucket_object: str) -> bool:
        """
        Check if an s3 bucket object already exists
        """
        existing_objects = self.get_bucket_prefixes()

        if bucket_object in existing_objects:
            return True
        else:
            return False

    def create_object(
        self, object_prefix: str, bucket: str = "defichain-analyzer"
    ) -> None:
        """
        Create bucket object in case it does not exist
        """
        self.client.put_object(Bucket=bucket, Key=(object_prefix + "/"))

    def create_bucket_objects(self, bucket: str = "defichain-analyzer") -> None:
        """
        Create bucket objects for all major dataframes retrieved
        """
        year, month, day = self.today.year, self.today.month, self.today.day
        date_path = f"{year}/{month}/{day}"

        _objects = [
            self.config["AWS"]["TOKENS_PREFIX"],
            self.config["AWS"]["POOL_PAIRS_PREFIX"],
            self.config["AWS"]["YIELD_FARMING_PREFIX"],
        ]
        objects = [f"{o}{date_path}/" for o in _objects]

        for obj in objects:
            object_exists = self.check_object_existence(bucket_object=obj)
            if not object_exists:
                logging.info(f"{obj} is being created...")
                self.create_object(object_prefix=obj, bucket=bucket)
            else:
                logging.info(f"{obj} already exists...")
