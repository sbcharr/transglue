import logging as log
import configparser
import argparse
import os


__config = configparser.ConfigParser()
__config.read(os.path.join(os.getcwd(), 'dl.cfg'))

os.environ['GLUE_DB_HOST'] = __config['postgres-db']['GLUE_DB_HOST']
os.environ['GLUE_JOBS_DB'] = __config['postgres-db']['GLUE_JOBS_DB']
os.environ['GLUE_POSTGRES_USER'] = __config['postgres-db']['GLUE_POSTGRES_USER']
os.environ['GLUE_POSTGRES_PASSWORD'] = __config['postgres-db']['GLUE_POSTGRES_PASSWORD']

os.environ['AWS_ACCESS_KEY_ID'] = __config['aws-creds']['aws_access_key_id']
os.environ['AWS_SECRET_ACCESS_KEY'] = __config['aws-creds']['aws_secret_access_key']
os.environ['REGION_NAME'] = __config['aws-creds']['region_name']
os.environ['IAM_ROLE_DATA_COPY'] = __config['aws-creds']['iam-role_data_copy']

os.environ['SQS_QUEUE_NAME'] = __config['aws-sqs']['sqs_queue_name']

os.environ['TEMP_S3_BUCKET'] = __config['temp-s3']['s3_bucket']


def flag_parser():
    """
    A function to parse parameterized input to command line arguments. It returns the object
    containing values of each input argument in a key/value fashion.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--jobName", help="(optional for admin) job name to execute")
    parser.add_argument("--jobInstance", help="(optional for admin) sequence number of job instance")
    parser.add_argument("--userType", help="user who runs this job, one of 'admin' or 'user'")
    parser.add_argument("--maxDpu", help="(optional) max dpu that AWS Glue uses, available only with user type 'user'")
    parser.add_argument("--logLevel", help="(optional) log level, values are 'debug', 'info', \
                                           'warning', 'error', 'critical'")

    args = parser.parse_args()

    return args


def set_logger(log_level):
    """
    Main logger set for the program. Default log level is set to INFO.
    """
    log_level_switcher = {
        'debug': log.DEBUG,
        'info': log.INFO,
        'warning': log.WARNING,
        'error': log.ERROR,
        'critical': log.CRITICAL
    }
    log.basicConfig(level=log_level_switcher.get(log_level, log.INFO))


def setup():
    args = flag_parser()
    set_logger()

    return args
