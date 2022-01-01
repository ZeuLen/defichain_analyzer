import configparser

import log
from aws import BucketManager
from data_collector import Datacollector

logger = log.setup_custom_logger("root")


def main() -> None:
    logger.info("Starting to retrieve defichain data...")

    logger.info("Reading configuration...")
    config = configparser.ConfigParser()
    config.read("config.ini")

    logger.info("Reading configuration...")
    resource_manager = BucketManager(config)
    resource_manager.create_bucket_objects()

    defichain_collector = Datacollector()
    defichain_collector.persist_csv_to_s3()


if __name__ == "__main__":
    main()
