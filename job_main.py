import logging as log
import time
from commons import commons as c
from service import database_service as db_service, glue_service


def run_glue_job(job_name, job_instance, postgres_instance, glue_instance, max_dpu=None):
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
    # job_run_id = "XYZ123498"
    postgres_instance.update_job_instance(job_name, job_instance, job_run_id, 'UNKNOWN')

    while True:
        glue_job_status = glue_instance.get_glue_job_status(job_run_id)
        # glue_job_status = 'SUCCEEDED'
        if glue_job_status == 'SUCCEEDED':
            log.info("job {} with job run id {} is successfully completed".format(job_name, job_run_id))
            postgres_instance.update_job_instance(job_name, job_instance, job_run_id, glue_job_status)
            break
        elif glue_job_status in ['FAILED', 'TIMEOUT']:
            log.error("job {} with job run id {} is failed".format(job_name, job_run_id))
            postgres_instance.update_job_instance(job_name, job_instance, job_run_id, glue_job_status)
        time.sleep(20)


def main():
    # TODO: input validation
    args = c.setup()
    job_name = args.jobName
    job_instance = args.jobInstance
    max_dpu = args.maxDpu

    postgres_instance = db_service.PostgresService()
    glue_instance = glue_service.GlueJobService()

    if args.userType != 'user':
        log.error("only valid option is user; invalid --userType, type -h for help")
        raise

    if max_dpu is not None:
        run_glue_job(job_name, job_instance, postgres_instance, glue_instance, max_dpu)
    else:
        run_glue_job(job_name, job_instance, postgres_instance, glue_instance)


if __name__ == "__main__":
    main()
