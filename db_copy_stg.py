from botocore.exceptions import ClientError
import logging as log
import time
import base64
from commons import commons as c
from aws import sqs_service, database_service


def main():
    temp_s3_bucket = c.os.environ['TEMP_S3_BUCKET']
    iam_role = c.os.environ['IAM_ROLE_DATA_COPY']

    args = c.setup()
    postgres_instance = database_service.PostgresDBService()
    sqs_instance = sqs_service.AwsSqsService()

    encoded_queue_name = base64.b64encode(args.jobName + "#" + args.jobInstance) + ".fifo"

    queue_url = sqs_instance.sqs_client.get_queue_url(
        QueueName=encoded_queue_name,
    )

    # Assign this value before running the program
    sqs_queue_url = queue_url['QueueUrl']

    # Retrieve SQS messages
    msg = ""
    i = 0
    while True:
        if msg != "done!":
            try:
                msg = sqs_instance.retrieve_sqs_message(sqs_queue_url)
            except ClientError as e:
                log.error(e)
                time.sleep(15)
                continue

            log.info("loading data into table {}...".format(msg['Body'].strip()))
            postgres_instance.copy_to_database(msg.strip(), temp_s3_bucket, iam_role)

            log.info("successfully loaded data into table {}".format(msg['Body'].strip()))
            postgres_instance.update_job_details(args.jobName, args.jobInstance, msg.strip())

            log.info("updated table name {} in job_details".format(msg['Body'].strip()))

            msg_receipt_handle = msg['ReceiptHandle']
            try:
                sqs_instance.delete_sqs_message(sqs_queue_url, msg_receipt_handle)
            except ClientError as e:
                log.error(e)
                raise
            print("message {} deleted from the queue".format(msg['Body'].strip()))
            i += 1
        else:
            if i != int(args.batchSize):
                log.error("not all {} values were received".format(args.batchSize))
                raise
            break

    log.info("job {} with instance id {} is completed successfully".format(args.jobName, args.jobInstance))


if __name__ == "__main__":
    main()


