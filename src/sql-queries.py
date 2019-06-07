create_control_schema="CREATE SCHEMA IF NOT EXISTS job_control;"
create_table_jobs="CREATE TABLE IF NOT EXISTS jobs ( \
    job_name text PRIMARY KEY, \
    job_description text, \
    log_uri text, \
    role_arn text NOT NULL, \
    max_concurrent_runs int DEFAULT 1, \
    comman_name text DEFAULT 'glueetl', \
    script_location text not null, \
    max_retries int DEFAULT 1, \
    timeout_minutes int DEFAULT 240, \
    max_capacity numeric(3,1), \
    notify_delay_after int, \
    tags text NOT NULL, \
    created_timestamp timestamp DEFAULT now() NOT NULL, \
    modified_timestamp timestamp DEFAULT now() NOT NULL, \
    last_run_timestamp timestamp DEFAULT '1970-01-01 00:00:00' NOT NULL, \
    is_active char(1) \
);"

create_table_job_instances="CREATE TABLE IF NOT EXISTS job_instances ( \
    job_name text REFERENCES jobs(job_name), \
    job_instance int NOT NULL, \
    job_run_id text, \
    status text, \
    PRIMARY KEY (job_name, job_instance) \
);"

create_table_job_details="CREATE TABLE IF NOT EXISTS job_details ( \
    job_name text REFERENCES jobs(job_name), \
    job_instance int REFERENCES job_instances(job_instance), \
    table_name text, \
    created_timestamp, \
    last_run_timestamp, \
    PRIMARY KEY (job_name, job_instance, table_name) \
);"