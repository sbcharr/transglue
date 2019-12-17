import time
from commons import commons


CONFIG = commons.get_config()
log = commons.get_logger(CONFIG['logLevel'], CONFIG['logFile'])


def validate_input_table_with_metadata(postgres_instance, job_name, job_instance, target_tables):
    list_target_tables = target_tables.split('|')
    tables_formatted = ""
    for table in list_target_tables:
        tables_formatted += "'" + table.strip() + "',"

    actual_count_from_db = postgres_instance.check_table_exists.format(job_name, job_instance,
                                                                       tables_formatted.strip(','))
    if actual_count_from_db != len(list_target_tables):
        return False

    return True


def run_glue_job(job_name, job_instance, postgres_instance, glue_instance, tables=None, max_dpu=None, incr_from="",
                 incr_to="", context=""):
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
    if tables is None:
        target_tables = postgres_instance.get_job_details(job_name, job_instance)
    else:
        # pass tables as a '|' separated string such as "tableA:tableB:tableC" etc.
        target_tables = tables
        match_table_count = validate_input_table_with_metadata(postgres_instance, job_name, job_instance,
                                                               target_tables)

        if not match_table_count:
            log.error("supplied table/tables are not in metadata, please supply correct table names, exiting...")
            return

    job_run_id = glue_instance.start_glue_job(job_name, job_instance, max_dpu, target_tables, incr_from, incr_to,
                                              context)
    # job_run_id = "XYZ123498"
    postgres_instance.update_job_instance(job_name, job_instance, job_run_id, 'UNKNOWN')

    while True:
        glue_job_status = glue_instance.get_glue_job_status(job_run_id)
        if glue_job_status in ['SUCCEEDED', 'FAILED', 'TIMEOUT']:
            log.info("job {} with job run id {} is completed with status".format(job_name, job_run_id, glue_job_status))
            postgres_instance.update_job_instance(job_name, job_instance, job_run_id, glue_job_status)
            break
        # elif glue_job_status in ['FAILED', 'TIMEOUT']:
        #     log.error("job {} with job run id {} is failed".format(job_name, job_run_id))
        #     postgres_instance.update_job_instance(job_name, job_instance, job_run_id, glue_job_status)
        # check every 30 seconds on the completion of the job
        time.sleep(30)


# def main():
#     # TODO: input validation
#     args = c.setup()
#     job_name = args.jobName
#     job_instance = args.jobInstance
#     max_dpu = args.maxDpu
#
#     postgres_instance = db_service.PostgresService()
#     glue_instance = glue_service.GlueJobService()
#
#     if args.userType != 'user':
#         log.error("only valid option is user; invalid --userType, type -h for help")
#         raise
#
#     if max_dpu is not None:
#         run_glue_job(job_name, job_instance, postgres_instance, glue_instance, max_dpu)
#     else:
#         run_glue_job(job_name, job_instance, postgres_instance, glue_instance)
#
#
# if __name__ == "__main__":
#     main()
