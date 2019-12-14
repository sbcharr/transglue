import logging as log
from logging.handlers import TimedRotatingFileHandler
# import configparser
# import argparse
import os
import sys
import json


LOG_FORMATTER = log.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(funcName)s - %(message)s")
# __config = configparser.ConfigParser()
# __config.read(os.path.join(os.getcwd(), 'dl.cfg'))
#
# os.environ['REGION_NAME'] = __config['aws-creds']['region_name']
# os.environ['IAM_ROLE'] = __config['aws-creds']['iam-role']

# os.environ['SQS_QUEUE_NAME'] = __config['aws-sqs']['sqs_queue_name']

# os.environ['TEMP_S3_BUCKET'] = __config['temp-s3']['s3_bucket']


def get_config():
    with open(os.path.join(os.getcwd(), 'config.json')) as f:
        conf = json.load(f)

        return conf


# def flag_parser():
#     """
#     A function to parse parameterized input to command line arguments. It returns the object
#     containing values of each input argument in a key/value fashion.
#     """
#     parser = argparse.ArgumentParser()
#     parser.add_argument("--jobName", help="(optional for admin) job name to execute")
#     parser.add_argument("--jobInstance", help="sequence number of job instance")
#     # parser.add_argument("--userType", help="user who runs this job, one of 'admin' or 'user'")
#     parser.add_argument("--maxDpu", help="(optional) max dpu that AWS Glue uses, available only with user type 'user'")
#     # parser.add_argument("--logLevel", help="(optional) log level, values are 'debug', 'info', \
#     #                                       'warning', 'error', 'critical'")
#     # parser.add_argument("--from_date", help="from data date to be passed to the Glue script")
#     # parser.add_argument("--to_date", help="to date to be passed to the Glue script")
#     # parser.add_argument("--batchSize", help="(optional for admin) number of tables to process") # used for db copy
#
#     args = parser.parse_args()
#
#     return args


def get_console_handler():
    console_handler = log.StreamHandler(sys.stdout)
    console_handler.setFormatter(LOG_FORMATTER)

    return console_handler


def get_file_handler(log_file):
    file_handler = TimedRotatingFileHandler(log_file, when='midnight')
    file_handler.setFormatter(LOG_FORMATTER)

    return file_handler


def get_logger(log_level, log_file=None):
    log_level_switcher = {
        'debug': log.DEBUG,
        'info': log.INFO,
        'warning': log.WARNING,
        'error': log.ERROR,
        'critical': log.CRITICAL
    }
    logger = log.getLogger(__name__)
    logger.setLevel(log_level_switcher.get(log_level, log.INFO))

    if log_file is None or log_file == "":
        logger.addHandler(get_console_handler())
    else:
        logger.addHandler(get_file_handler(log_file))
    # don't propagate to the parent
    logger.propagate = False

    return logger



