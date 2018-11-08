import json
import logging
import ccc_1min_downloader

from datetime import datetime

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def run(event, context):
    config_path = 's3://douglas-am-marketdata/cryptocurrencies/cryptocompare/ccc_1min_config.json'
    ccc_1min_downloader.download(config_path)
    logger.info("Downloader " + context.function_name + " ran at " + str(datetime.utcnow()))
