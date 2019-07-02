from botocore.exceptions import ClientError
import logging as log
import time
from commons import commons as c
from aws import sqs_service, database_service


def main():
    args = c.setup()

    job_name = args.jobName
    job_instance = args.jobInstance
    batch_size = args.batchSize
    temp_s3_bucket = c.os.environ['TEMP_S3_BUCKET']
    iam_role = c.os.environ['IAM_ROLE']

    postgres_instance = database_service.PostgresDBService()
    sqs_instance = sqs_service.AwsSqsService()

    queue_name = job_name + "_" + job_instance + ".fifo"

    queue_url = sqs_instance.sqs_client.get_queue_url(
        QueueName=queue_name,
    )

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

            table = msg['Body'].strip()
            job_name = args.jobName
            job_instance = args.jobInstance

            log.info("truncating table {}".format(table))
            postgres_instance.truncate_stage_table(table)

            log.info("loading data into table {}...".format(msg['Body'].strip()))
            postgres_instance.copy_to_database(table, temp_s3_bucket, iam_role)

            log.info("successfully loaded data into table {}".format(msg['Body'].strip()))
            postgres_instance.update_job_details(job_name, job_instance, table)

            log.info("updated table name {} in job_details".format(table))

            msg_receipt_handle = msg['ReceiptHandle']
            try:
                sqs_instance.delete_sqs_message(sqs_queue_url, msg_receipt_handle)
            except ClientError as e:
                log.error(e)
                raise
            print("message {} deleted from the queue".format(table))
            i += 1
        else:
            if i != int(batch_size):
                log.error("not all {} messages were received".format(batch_size))
                raise
            break

    log.info("job {} with instance id {} is completed successfully".format(job_name, job_instance))


if __name__ == "__main__":
    main()


