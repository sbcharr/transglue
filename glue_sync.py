import logging as log
import pandas as pd
from commons import commons as c
from aws import database_service as db_service, glue_service

"""
    This script is to be scheduled as a separate job, e.g. every 4 hours. When it runs, it updates the status of
    jobs available in db with that of AWS Glue. Make sure to pass --userType as 'admin', else it will thrown an error
"""


def sync_jobs(postgres_instance, glue_instance):
    '''sync_job runs as a separate job on a periodic basic to sync up job info between Postgres
    db and AWS Glue. It receives a postgres instance and a aws instance as its parameters. Sync
    job should be executed by an Admin user.
    '''

    role_arn = c.os.environ['IAM_ROLE']

    # Get all relevant aws jobs from Postgres db
    df_job = postgres_instance.get_glue_jobs_from_db()
    if df_job.shape[0] == 0:
        log.info("empty jobs table, exiting...")
        return

    # get_glue_jobs returns all job names from AWS Glue as a list
    df_glue_jobs = pd.DataFrame(glue_instance.get_glue_jobs(), columns=['job_name'])
    print(df_glue_jobs)
    # pandas data frame is used to identify jobs that are to be created, updated and deleted in AWS Glue Service
    df_temp = pd.merge(df_job, df_glue_jobs, left_on='job_name', right_on='job_name', how='outer', indicator=True)

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

    log.info("successfully synchronized job between database and AWS Glue")


def main():
    args = c.setup()

    postgres_instance = db_service.PostgresDBService()
    glue_instance = glue_service.GlueJobService()

    if args.userType != 'admin':
        log.error("only valid option is admin; invalid --userType, type -h for help")
        raise

    # creates necessary db objects to control various Glue jobs
    postgres_instance.create_postgres_db_objects()

    # syncs jobs between control tables and aws glue
    sync_jobs(postgres_instance, glue_instance)


if __name__ == "__main__":
    main()
