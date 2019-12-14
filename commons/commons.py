import logging as log
import configparser
import argparse
import os


__config = configparser.ConfigParser()
__config.read(os.path.join(os.getcwd(), 'dl.cfg'))

os.environ['REGION_NAME'] = __config['aws-creds']['region_name']
# os.environ['IAM_ROLE'] = __config['aws-creds']['iam-role']

# os.environ['SQS_QUEUE_NAME'] = __config['aws-sqs']['sqs_queue_name']

# os.environ['TEMP_S3_BUCKET'] = __config['temp-s3']['s3_bucket']


def flag_parser():
    """
    A function to parse parameterized input to command line arguments. It returns the object
    containing values of each input argument in a key/value fashion.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--jobName", help="(optional for admin) job name to execute")
    parser.add_argument("--jobInstance", help="sequence number of job instance")
    # parser.add_argument("--userType", help="user who runs this job, one of 'admin' or 'user'")
    parser.add_argument("--maxDpu", help="(optional) max dpu that AWS Glue uses, available only with user type 'user'")
    # parser.add_argument("--logLevel", help="(optional) log level, values are 'debug', 'info', \
    #                                       'warning', 'error', 'critical'")
    # parser.add_argument("--from_date", help="from data date to be passed to the Glue script")
    # parser.add_argument("--to_date", help="to date to be passed to the Glue script")
    # parser.add_argument("--batchSize", help="(optional for admin) number of tables to process") # used for db copy

    args = parser.parse_args()

    return args


def set_logger(filename, log_level=log.INFO):
    """
    Main logger set for the program. Default log level is set to INFO.
    """
    log_format = "%(asctime)-15s %(message)s"
    log_level_switcher = {
        'debug': log.DEBUG,
        'info': log.INFO,
        'warning': log.WARNING,
        'error': log.ERROR,
        'critical': log.CRITICAL
    }
    log.basicConfig(filename=filename, filemode='a+', format=log_format, level=log_level_switcher.get(log_level, log.INFO))


# def setup(filename, log_level="info"):
#     # args = flag_parser()
#     set_logger(filename, log_level)
#
#     # return args
#
#
# def validate_input():
#     pass

