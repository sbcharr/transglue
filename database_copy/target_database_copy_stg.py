import boto3
from botocore.exceptions import ClientError
import logging as log
import configparser, argparse, os
import time
import base64
import psycopg2
import aws_glue_operations as agops



config = configparser.ConfigParser()
config.read(os.path.join(os.getcwd(), 'dl.cfg'))

os.environ['GLUE_DB_HOST'] = config['postgres-db']['GLUE_DB_HOST']
os.environ['GLUE_JOBS_DB'] = config['postgres-db']['GLUE_JOBS_DB']
os.environ['GLUE_POSTGRES_USER'] = config['postgres-db']['GLUE_POSTGRES_USER']
os.environ['GLUE_POSTGRES_PASSWORD'] = config['postgres-db']['GLUE_POSTGRES_PASSWORD']

os.environ['AWS_ACCESS_KEY_ID']=config['aws-creds']['aws_access_key_id']
os.environ['AWS_SECRET_ACCESS_KEY']=config['aws-creds']['aws_secret_access_key']
os.environ['REGION_NAME']=config['aws-creds']['region_name']


def flag_parser():
    """
    A function to parse parameterized input to command line arguments. It returns the object
    containing values of each input argument in a key/value fashion.
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("--jobName", help="(optional for admin) job name to execute")
    parser.add_argument("--jobInstance", help="(optional for admin) sequence number of job instance")
    #    parser.add_argument("--batchSize", help ="number of target tables to process")
    parser.add_argument("--logLevel", help="(optional) log level, values are 'debug', 'info', 'warning', 'error', 'critical'")

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


def create_sqs_client():
    sqs_client = boto3.client('sqs', region_name=os.environ['REGION_NAME'],
        aws_access_key_id=os.environ['AWS_SECRET_ACCESS_KEY'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY']
    )

    return sqs_client

def retrieve_sqs_messages(sqs_client, sqs_queue_url, num_msgs=1, wait_time=0, visibility_time=1):
    """Retrieve messages from an SQS queue

    The retrieved messages are not deleted from the queue.

    :param sqs_queue_url: String URL of existing SQS queue
    :param num_msgs: Number of messages to retrieve (1-10)
    :param wait_time: Number of seconds to wait if no messages in queue
    :param visibility_time: Number of seconds to make retrieved messages
        hidden from subsequent retrieval requests
    :return: List of retrieved messages. If no messages are available, returned
        list is empty. If error, returns None.
    """

    # Validate number of messages to retrieve
    if num_msgs < 1:
        num_msgs = 1
    elif num_msgs > 10:
        num_msgs = 10

    # Retrieve messages from an SQS queue

    try:
        msgs = sqs_client.receive_message(QueueUrl=sqs_queue_url,
                                            MaxNumberOfMessages=num_msgs,
                                            WaitTimeSeconds=wait_time,
                                            VisibilityTimeout=visibility_time)
    except ClientError as e:
        log.error(e)
        return None

    # Return the list of retrieved messages
    return msgs['Messages']


def delete_sqs_message(sqs_client, sqs_queue_url, msg_receipt_handle):
    """Delete a message from an SQS queue

    :param sqs_queue_url: String URL of existing SQS queue
    :param msg_receipt_handle: Receipt handle value of retrieved message
    """

    # Delete the message from the SQS queue
    
    sqs_client.delete_message(QueueUrl=sqs_queue_url,
                                ReceiptHandle=msg_receipt_handle)


def copy_to_database():
    pass

def update_job_details():
    pass


def main():
    args = flag_parser()        # setup parser to parse named arguments
    set_logger(args.logLevel)       # set logger for the app

    try:
        sqs_client = create_sqs_client()
    except ClientError as e:
        log.error(e)
        raise

    string_to_encode = args.jobName + "#" + args.jobInstance
    encoded_str = base64.b64encode(string_to_encode)

    queue_url = sqs_client.get_queue_url(
        QueueName=encoded_str,
    )
    # Assign this value before running the program
    sqs_queue_url = queue_url['QueueUrl']

    # Retrieve SQS messages
    while True:
        time.sleep(60)
        try:
            msg = retrieve_sqs_messages(sqs_client, sqs_queue_url)
        except ClientError as e:
            log.error(e)
            continue
        
        if msg['Body'] != 'done!':
            msg_receipt_handle =  msg['ReceiptHandle']
            try:
                delete_sqs_message(sqs_client, sqs_queue_url, msg_receipt_handle)
            except ClientError as e:
                log.error(e)
                raise
            print("message {} deleted from the queue".format(msg['Body']))
        else:
            break


