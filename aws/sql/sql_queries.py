create_control_schema = "CREATE SCHEMA IF NOT EXISTS job_control_admin;"
create_table_jobs = "CREATE TABLE IF NOT EXISTS jobs ( \
    job_name text PRIMARY KEY, \
    job_description text, \
    log_uri text, \
    role_arn text NOT NULL, \
    max_concurrent_runs int DEFAULT 1, \
    command_name text DEFAULT 'glueetl', \
    script_location text not null, \
    max_retries int DEFAULT 1, \
    timeout_minutes int DEFAULT 240, \
    max_capacity numeric(3,1), \
    notify_delay_after int, \
    tags text NOT NULL, \
    created_timestamp timestamp DEFAULT now() NOT NULL, \
    modified_timestamp timestamp DEFAULT now() NOT NULL, \
    last_sync_timestamp timestamp DEFAULT '1970-01-01 00:00:00' NOT NULL, \
    is_active char(1) DEFAULT 'N' \
);"

create_table_job_instances = "CREATE TABLE IF NOT EXISTS job_instances ( \
    job_name text NOT NULL, \
    job_instance int NOT NULL, \
    job_run_id text, \
    status text, \
    PRIMARY KEY (job_name, job_instance), \
    FOREIGN KEY (job_name) REFERENCES jobs (job_name) \
);"

create_table_job_details = "CREATE TABLE IF NOT EXISTS job_details ( \
    job_name text NOT NULL, \
    job_instance int NOT NULL, \
    table_name text, \
    created_timestamp timestamp DEFAULT now(), \
    last_run_timestamp timestamp DEFAULT '1970-01-01 00:00:00' NOT NULL, \
    PRIMARY KEY (job_name, job_instance, table_name), \
    FOREIGN KEY (job_name, job_instance) REFERENCES job_instances (job_name, job_instance) \
);"

use_schema = "SET search_path TO job_control_admin;"

select_from_jobs = "select jobs.* from jobs join job_instances j on jobs.job_name = j.job_name" \
                   " where j.job_instance = '{}';"
select_from_job_instances = "select job_run_id, status from job_instances where job_name = '{}' \
                            and job_instance = '{}';"
select_from_job_details = "select table_name from job_details where job_name = '{}' and job_instance = '{}'"

insert_into_jobs = "insert into jobs(job_name, job_description, log_uri, role_arn, max_concurrent_runs, \
                script_location, max_retries, timeout_minutes, max_capacity, notify_delay_after, tags, \
                is_active) values('test-job-nycitytaxi-01', 'nycitytaxi test job', null, \
                'arn:aws:iam::350072545445:role/role_glues3redshift', 2, \
                's3://sb-glue-etl-scripts/scripts/el_nycitytaxi_to_dl.py', 1, 20, 2, null, 'data-engineering', 'Y');"

insert_into_job_instances = "insert into job_instances (job_name, job_instance) values('test-job-nycitytaxi-01', 1);"

insert_into_job_details_01 = "insert into job_details (job_name, job_instance, table_name) \
                            values('test-job-nycitytaxi-01', 1, 'job_control_admin.green_taxi');"

insert_into_job_details_02 = "insert into job_details (job_name, job_instance, table_name) \
                            values('test-job-nycitytaxi-01', 1, 'job_control_admin.fhv');"

update_table_jobs = "update jobs set last_sync_timestamp = '{}' where is_active = 'Y'"
update_table_job_instances = "update job_instances set job_run_id = '{}', status = '{}' where job_name = '{}' \
                            and job_instance = '{}"
update_table_job_details = "update job_details set last_run_timestamp = '{}' where job_name = '{}' \
                            and job_instance = '{}' and table_name = '{}'"

load_data_to_table = "copy {} from '{}' iam_role '{}' format as parquet;"

create_sql_stmts = [create_control_schema, use_schema, create_table_jobs,
                    create_table_job_instances, create_table_job_details]

