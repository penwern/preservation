import argparse
import json
import logging
import sys
import time

from config import LOG_DIRECTORY
from preservation.preservation import Preservation
from preservation.preservation import process_node

logger = logging.getLogger("preservation")
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(f'{LOG_DIRECTORY}/preservation.log')
file_handler.setFormatter(logging.Formatter("%(asctime)s %(filename)s:%(lineno)d %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
logger.addHandler(file_handler)

def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Curate Preservation')
    parser.add_argument('-c', '--config_id', help='Config ID', type=int, required=True)
    parser.add_argument('-n', '--nodes', help='Array of node submitted from Curate', required=True)
    parser.add_argument('-u', '--user', help='User', required=True)
    args = parser.parse_args()
    return args
    

def main():
    # logger.debug(f"Arguments: {sys.argv}")
    args = parse_arguments()
    preserver = Preservation(config_id = args.config_id, user=args.user)

    # logger.debug(args.nodes)
    for node in json.loads(args.nodes):
        processing_directory = preserver.get_new_processing_directory()
        try:
            process_node(preserver, node, processing_directory)
        except Exception as e:
            continue

if __name__ == '__main__':
    try:
        logger.info(' =============== NEW =============== ')
        start = time.time()
        main()
    except Exception as e:
        logger.error(e)
        end = time.time()
        length = end - start
        logger.info(f' ============= FALIED in {length:.2f} seconds ============= \n')
        raise
    end = time.time()
    length = end - start
    logger.info(f' ============= COMPLETED in {length:.2f} seconds ============= \n')
