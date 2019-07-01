import logging as log
import time
from commons import commons as c
from aws import database_service as db_service, glue_service, sqs_service


def run_glue_job(job_name, job_instance, max_dpu, postgres_instance, glue_instance):

    # get job run id and status of previous job before running a new instance. This is to ensure
    # that previous job has completed.
    job_run_id, status = postgres_instance.get_job_status(job_name, job_instance)
    if job_run_id is not None:
        if status in ['STARTING', 'RUNNING', 'STOPPING', 'UNKNOWN']:
            job_status = glue_instance.get_glue_job_status(job_run_id)
            if job_status in ['STARTING', 'RUNNING', 'STOPPING']:
                log.error("a previous instance of job {}#{} with job_run_id={} is still not completed, exiting..."
                          .format(job_name, job_instance, job_run_id))
                raise

    target_tables = postgres_instance.get_job_details(job_name, job_instance)

    job_run_id = glue_instance.start_glue_job(job_name, job_instance, max_dpu, target_tables)
    postgres_instance.update_job_instance(job_name, job_instance, job_run_id, 'UNKNOWN')

    while True:
        glue_job_status = glue_instance.get_glue_job_status(job_run_id)
        if glue_job_status == 'SUCCEEDED':
            log.info("job {} with job run id {} is successfully completed".format(job_name, job_run_id))
            postgres_instance.update_job_instance(job_name, job_instance, job_run_id, glue_job_status)
            break
        elif glue_job_status in ['FAILED', 'TIMEOUT']:
            log.error("job {} with job run id {} is failed".format(job_name, job_run_id))
            postgres_instance.update_job_instance(job_name, job_instance, job_run_id, glue_job_status)
        time.sleep(20)


def process_sqs_queue(job_name, job_instance, sqs_instance):
    # For SQS FIFO
    queue_name = job_name + "_" + job_instance + ".fifo"

    queue_url = sqs_instance.get_queue_url(queue_name)

    if queue_url:
        sqs_queue_url = queue_url['QueueUrl']

        # deletes the existing queue
        sqs_instance.delete_fifo_queue(sqs_queue_url)
        log.info("deleted fifo queue {}".format(queue_name))

    # creates sqs fifo queue
    sqs_instance.create_fifo_queue(queue_name)
    log.info("created fifo queue {}".format(queue_name))


def main():
    args = c.setup()

    postgres_instance = db_service.PostgresDBService()
    glue_instance = glue_service.GlueJobService()
    sqs_instance = sqs_service.AwsSqsService()

    if args.userType != 'user':
        log.error("only valid option is user; invalid --userType, type -h for help")
        raise

    process_sqs_queue(args.jobName, args.jobInstance, sqs_instance)

    run_glue_job(args.jobName, args.jobInstance, args.maxDpu, postgres_instance, glue_instance)


if __name__ == "__main__":
    main()
