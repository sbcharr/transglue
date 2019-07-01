import logging as log
import time
import pandas as pd
from commons import commons as c
from aws import database_service as db_service, glue_service, sqs_service


def sync_jobs(job_instance, postgres_instance, glue_instance):
    '''sync_job runs as a separate job on a periodic basic to sync up job info between Postgres
    db and AWS Glue. It receives a postgres instance and a aws instance as its parameters. Sync
    job should be executed by an Admin user.
    '''

    role_arn = c.os.environ['IAM_ROLE']

    # Get all relevant aws jobs from Postgres db
    df_jobs_all = postgres_instance.get_glue_jobs_from_db(job_instance)
    if df_jobs_all.shape[0] == 0:
        log.info("empty jobs table, exiting function...")
        return

    # get_glue_jobs returns all job names from AWS Glue as a list
    df_glue_jobs = pd.DataFrame(glue_instance.get_glue_jobs(), columns=['job_name'])
    print(df_glue_jobs)
    # pandas data frame is used to identify jobs that are to be created, updated and deleted in AWS Glue Service
    df_temp = pd.merge(df_jobs_all, df_glue_jobs, left_on='job_name', right_on='job_name', how='outer', indicator=True)

    df_insert_recs = df_temp[(df_temp['_merge'] == 'left_only') & (df_temp['is_active'] == 'Y')]

    # TODO: edge case: what if someone updates the jobs table during execution of a job? this will make
    # modified_timestamp < last_run_timestamp and as a result the job will never update
    # solution: make the update operation on jobs table async and it should only execute once that particular job
    # is not running irrespective of when the update request is submitted.

    df_update_recs = df_temp[(df_temp['_merge'] == 'both') & (df_temp['is_active'] == 'Y') &
                             (df_temp['modified_timestamp'] > df_temp['last_sync_timestamp'])]

    df_delete_recs = df_temp[(df_temp['_merge'] == 'both') & (df_temp['is_active'] != 'Y')]
    # print(df_delete_recs)
    # print(df_insert_recs.dtypes)

    # delete operation to delete any inactive job
    for _, row in df_delete_recs.iterrows():
        log.info("deleting job {}...".format(row['job_name']))
        glue_instance.delete_glue_job(row['job_name'])

    # TODO: edge case: Check whether delete operator deletes a running instance of the job or not

    # create operation to create any new job that is inserted in Postgres db
    for _, row in df_insert_recs.iterrows():
        log.info("creating job {}...".format(row['job_name']))
        glue_instance.create_glue_job(
            row['job_name'],
            row['job_description'],
            role_arn,
            row['script_location'],
            row['command_name'],
            row['max_concurrent_runs'],
            row['max_retries'],
            row['timeout_minutes'],
            row['max_capacity']
        )

    # update any existing job whose definition has been changed recently in Postgres db
    for _, row in df_update_recs.iterrows():
        log.info("updating job {}...".format(row['job_name']))
        glue_instance.update_glue_job(
            row['job_name'],
            row['job_description'],
            role_arn,
            row['script_location'],
            row['command_name'],
            row['max_concurrent_runs'],
            row['max_retries'],
            row['timeout_minutes'],
            row['max_capacity']
        )

    postgres_instance.update_jobs_table()

    log.info("successfully synchronized jobs between database and AWS Glue")


def main_admin(job_name, job_instance, postgres_instance, glue_instance, sqs_instance):
    # creates necessary db objects to control various Glue jobs
    postgres_instance.create_postgres_db_objects()

    # syncs jobs between control tables and aws glue
    sync_jobs(job_instance, postgres_instance, glue_instance)

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


def main_user(job_name, job_instance, max_dpu, postgres_instance, glue_instance):

    # get job run id and status of previous job before running a new instance. This is to ensure
    # that previous job has completed.
    job_run_id, status = postgres_instance.get_job_status(job_name, job_instance)
    if status != 'completed':
        while True:
            job_status = glue_instance.get_glue_job_status(job_run_id)

            if job_status == 'SUCCEEDED':
                log.info("job {} with job run id {} is successfully completed".format(job_name, job_run_id))
                break
            time.sleep(20)

        # update control table with job status, either in-progress or completed
        postgres_instance.update_job_instance(job_name, job_instance, job_run_id, job_status_ctx=0)

    target_tables = postgres_instance.get_job_details(job_name, job_instance)
    job_run_id = glue_instance.start_glue_job(job_name, job_instance, max_dpu, target_tables)
    postgres_instance.update_job_instance(job_name, job_instance, job_run_id, job_status_ctx=1)

    while True:
        job_status = glue_instance.get_glue_job_status(job_run_id)
        if job_status == 'SUCCEEDED':
            log.info("job {} with job run id {} is successfully completed".format(job_name, job_run_id))
            break
        time.sleep(20)

    postgres_instance.update_job_instance(job_name, job_instance, job_run_id, job_status_ctx=0)

    return


def main():
    args = c.setup()

    postgres_instance = db_service.PostgresDBService()
    glue_instance = glue_service.GlueJobService()
    sqs_instance = sqs_service.AwsSqsService()

    if args.userType == 'admin':
        main_admin(args.jobName, args.jobInstance, postgres_instance, glue_instance, sqs_instance)
    elif args.userType == 'user':
        main_user(args.jobName, args.jobInstance, args.maxDpu, postgres_instance, glue_instance)
    else:
        raise ValueError("invalid --userType, type -h for help")


if __name__ == "__main__":
    main()
