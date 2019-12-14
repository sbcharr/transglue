create_control_schema = "CREATE SCHEMA IF NOT EXISTS job_control_admin;"

create_table_jobs = "CREATE TABLE IF NOT EXISTS jobs ( \
    job_name varchar(50) PRIMARY KEY, \
    job_description varchar(100), \
    log_uri varchar(100), \
    glue_version varchar(5) not null, \
    max_concurrent_runs integer not null DEFAULT 1, \
    command_name varchar(10) not null DEFAULT 'glueetl', \
    script_location varchar(200) not null, \
    max_retries integer not null DEFAULT 1, \
    timeout_minutes integer not null DEFAULT 240, \
    max_capacity decimal not null, \
    notify_delay_after integer, \
    created_timestamp timestamp not null DEFAULT CURRENT_TIMESTAMP, \
    modified_timestamp timestamp not null DEFAULT CURRENT_TIMESTAMP, \
    last_sync_timestamp timestamp not null DEFAULT '1970-01-01 00:00:00', \
    is_active char(1) DEFAULT 'N');"

create_table_job_instances = "CREATE TABLE IF NOT EXISTS job_instances ( \
    job_name varchar(50), \
    job_instance integer, \
    job_run_id varchar, \
    status varchar(10), \
    PRIMARY KEY(job_name, job_instance), \
    FOREIGN KEY(job_name) REFERENCES jobs(job_name) );"

create_table_job_details = "CREATE TABLE IF NOT EXISTS job_details ( \
    job_name varchar(50), \
    job_instance integer, \
    table_name varchar, \
    created_timestamp timestamp not null DEFAULT CURRENT_TIMESTAMP, \
    last_run_timestamp timestamp, \
    reprocess_timestamp timestamp, \
    is_active char(1) DEFAULT 'N', \
    PRIMARY KEY(job_name, job_instance, table_name), \
    FOREIGN KEY(job_name, job_instance) REFERENCES job_instances(job_name, job_instance) );"

use_schema = "SET search_path TO job_control_admin;"

select_from_jobs = "select * from jobs;"

select_is_active_job = "select 1 from jobs where if exists (select a.job_name, b.job_instance \
                        from jobs a join job_instances b on a.job_name = b.job_name \
                        where a.job_name = '{}' and a.is_active = 'Y';"

select_from_job_instances = "select job_run_id, status from job_instances where job_name = '{}' \
                            and job_instance = '{}';"
select_from_job_details = "select table_name from job_details where job_name = '{}' and job_instance = {} \
                          and is_active = 'Y';"

update_table_jobs = "update jobs set last_sync_timestamp = '{}';"

update_table_job_instances = "update job_instances set job_run_id = '{}', status = '{}' where job_name = '{}' \
                            and job_instance = {};"
update_table_job_details = "update job_details set last_run_timestamp = '{}' where job_name = '{}' \
                            and job_instance = {} and table_name = '{}';"

truncate_table_stg = "truncate {};"

load_data_to_table = "copy {} from '{}' iam_role '{}' format as parquet;"

create_sql_stmts = [create_control_schema, use_schema, create_table_jobs,
                    create_table_job_instances, create_table_job_details]

