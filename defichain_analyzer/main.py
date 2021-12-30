import configparser

from aws import BucketManager
from datacollector import Datacollector


def main() -> None:
    # load config file
    config = configparser.ConfigParser()
    config.read("config.ini")

    # create required buckets
    resource_manager = BucketManager(config)
    resource_manager.create_bucket_objects()

    # retrieve defichain data and write to aws
    defichain_collector = Datacollector()
    defichain_collector.persist_csv_to_s3()


if __name__ == "__main__":
    main()
