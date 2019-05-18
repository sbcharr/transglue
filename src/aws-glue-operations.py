import boto3
from botocore.exceptions import ClientError
import logging as log
import argparse, os, sys, time
import psycopg2
import pandas as pd
import pandas.io.sql as sqlio
from datetime import datetime



def set_logger():
    log.basicConfig(level=log.INFO)

def set_flag_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--jobName", help="job name to execute")
    parser.add_argument("--jobInstance", help="sequence number of job instance")
    parser.add_argument("--userType", help="user who runs this job, one of admin or user")

    args = parser.parse_args()

    return args


class PostgresDBGlueService:
    def __init__(self):
        self.__host = os.environ['GLUE_DB_HOST']
        self.__dbname = os.environ['GLUE_JOBS_DB']
        self.__user = os.environ['GLUE_POSTGRES_USER']
        self.__password = os.environ['GLUE_POSTGRES_PASSWORD']
    

    def __create_db_conn(self, host, dbname, user, password):
        try: 
            conn = psycopg2.connect(f"host={host} dbname={dbname} user={user} password={password}")
        except psycopg2.Error as err: 
            log.info("Error: Could not make connection to the Postgres database")
            log.error(err)
            raise
        
        conn.set_session(autocommit=True)
        log.info("successfully created connection to Postgresql, autocommit is on")

        return conn
    

    def get_glue_jobs_from_db(self):
        conn = self.__create_db_conn(self.__host, self.__dbname, self.__user, self.__password)
        sql = "select * from public.jobs where is_active='Y';"
        try: 
            df = sqlio.read_sql_query(sql, conn)
        except Exception as err: 
            log.info("Error: select *")
            log.error(err)
            raise
        finally:
            conn.close()
            log.info("successfully closed the db connection")
        
        return df

    # TODO: create a separate variadic function encapsulating db operating to avoid repetitiveness

    def update_jobs_table(self):
        value = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        conn = self.__create_db_conn(self.__host, self.__dbname, self.__user, self.__password)
        cur = conn.cursor()
        
        sql = f"update public.jobs set last_run_timestamp = '{value}'"
        try:
            cur.execute(sql)
        except Exception as err:
            log.info("update public.jobs ...")
            log.error(err)
            raise
        finally:
            cur.close()
            conn.close()
            log.info("successfully closed the db connection")

        log.info("column 'last_run_timestamp' in 'jobs' table is sucessfully updated")

    def update_job_instance(self, job_name, job_instance, job_run_id, ctx=1):
        if ctx == 0:
            status = "completed"
        else:
            status = "in-progress"

        conn = self.__create_db_conn(self.__host, self.__dbname, self.__user, self.__password)
        cur = conn.cursor()
        sql = f"update public.job_instances set job_run_id = '{job_run_id}', status = '{status}' where job_name = '{job_name}' and job_instance = '{job_instance}'"
        try:
            cur.execute(sql)
        except Exception as err:
            log.info("update public.job_instances ...")
            log.error(err)
            raise
        finally:
            cur.close()
            conn.close()
            log.info("successfully closed the db connection")

        log.info("column 'job_run_id' in 'job_instances' table is sucessfully updated")
    '''
    def update_job_details(self, job_name, job_instance, job_run_id):
        conn = self.__create_db_conn(self.__host, self.__dbname, self.__user, self.__password)
        cur = conn.cursor()
        
        sql = f"update public.job_details set job_run_id = '{job_run_id}' where job_name = '{job_name}' and job_instance = '{job_instance}'"
        try:
            cur.execute(sql)
        except Exception as err:
            log.info("update public.job_details ...")
            log.error(err)
            raise
        finally:
            cur.close()
            conn.close()
            log.info("successfully closed the db connection")

        log.info("column 'job_run_id' in 'job_details' table is sucessfully updated")

    '''
    def get_job_status(self, job_name, job_instance):
        conn = self.__create_db_conn(self.__host, self.__dbname, self.__user, self.__password)
        cur = conn.cursor()
        
        sql = f"select job_run_id, status from public.job_instances where job_name = '{job_name}' and job_instance = '{job_instance}'"
        try:
            cur.execute(sql)
            row = cur.fetchall()
        except Exception as err:
            log.info("select public.job_instances ...")
            log.error(err)
            raise
        finally:
            cur.close()
            conn.close()
            log.info("successfully closed the db connection")

        return row[0], row[1]

    def get_job_details(self, job_name, job_instance):
        conn = self.__create_db_conn(self.__host, self.__dbname, self.__user, self.__password)
        cur = conn.cursor()
        
        sql = f"select table_name from public.job_details where job_name = '{job_name}' and job_instance = '{job_instance}'"
        try:
            cur.execute(sql)
            rows = cur.fetchall()
        except Exception as err:
            log.info("select public.job_instances ...")
            log.error(err)
            raise
        finally:
            cur.close()
            conn.close()
            log.info("successfully closed the db connection")

        tables = []
        for row in rows:
            tables.append(row[0])

        return tables

        

class AwsGlueService:
    def __init__(self):
        self.__client = boto3.client('glue')


    def get_glue_jobs(self):
        is_run = 0
        next_token = ""
        jobs = []
        try:
            while is_run == 0:
                r = self.__client.get_jobs(NextToken=next_token, MaxResults=100)
                for job in r['Jobs']:
                    jobs.append(job['Name'])
                if 'NextToken' in r:
                    next_token = r['NextToken']
                else:
                    is_run = 1
        except Exception as err:
            log.error(err)
            raise
       
        return jobs


    def create_glue_job(self, 
                        job_name, 
                        description, 
                        role, 
                        script_loc, 
                        command_name='glueetl', 
                        max_concurrent_runs=3, 
                        max_retries=1, 
                        timeout=180, 
                        max_capacity=2.0):
        try:
            _ = self.__client.create_job(
                Name=job_name,
                Description=description,
                Role=role,
                ExecutionProperty={
                    'MaxConcurrentRuns': max_concurrent_runs,
                },
                Command={'Name': command_name,
                        'ScriptLocation': script_loc
                        },
                MaxRetries=max_retries,
                Timeout=timeout,
                MaxCapacity=max_capacity
            )
        except Exception as err:
            log.error(err)
            raise

        log.info(f"glue job {job_name} is successfully created")
   
  
    def update_glue_job(self,     
                        job_name,
                        description, 
                        role, 
                        script_loc, 
                        command_name='glueetl', 
                        max_concurrent_runs=3, 
                        max_retries=1, 
                        timeout=180, 
                        max_capacity=2.0):
        try:
            _ = self.__client.update_job(
                JobName=job_name,
                JobUpdate={
                    'Description': description,
                    'Role': role,
                    'ExecutionProperty': {
                        'MaxConcurrentRuns': max_concurrent_runs
                    },
                    'Command': {
                        'Name': command_name,
                        'ScriptLocation': script_loc
                    },
                    'MaxRetries': max_retries,
                    'Timeout': timeout,
                    'MaxCapacity': max_capacity
                }
            )
        except Exception as err:
            log.error(err)
            raise

        log.info(f"glue job {job_name} is successfully updated")


    def delete_glue_job(self, job_name):
        try:
            _ = self.__client.delete_job(
                JobName=job_name
            )
        except Exception as err:
            log.error(err)
            raise

        log.info(f"glue job {job_name} is successfully deleted")


    def start_glue_job(self, job_name, job_instance, tables):
        try:
            r = self.__client.start_job_run(
                JobName=job_name,
                Arguments={
                    '--job-instance': job_instance,
                    '--tables': tables
                }
            )
        except Exception as err:
            log.error(err)
            raise

        log.info(f"glue job {job_name} is successfully started with jo id {r['JobRunId']}")

        return r['JobRunId']

    def get_glue_job_status(self, job_name, job_run_id):
        try:
            r = self.__client.get_job_run(JobName=job_name, RunId=job_run_id, PredecessorsIncluded=False)
        except Exception as err:
            log.error(err)
            raise

        return r['JobRun']['JobRunState'], r['JobRun']['ErrorMessage']


def sync_jobs(postgres_instance, glue_instance):
    '''sync_job runs as a separate job on a periodic basic to sync up job info between Postgres
    db and AWS Glue. It receives a postgres instance and a glue instance as its parameters. Sync
    job should be executed by an Admin user.
    '''

    # Get all glue jobs from Postgres db
    df_jobs = postgres_instance.get_glue_jobs_from_db()  

    # get_glue_jobs retuns all job names from AWS Glue as a list
    df_glue_jobs = pd.DataFrame(glue_instance.get_glue_jobs(), columns=['job_name'])  
    
    # pandas dataframe is used to identify jobs that are to be created, updated and deleted in AWS Glue Service
    df_temp = pd.merge(df_jobs, df_glue_jobs, left_on='job_name', right_on='job_name', how='outer', indicator=True)
    
    df_insert_recs = df_temp[df_temp['_merge'] == 'left_only']
    df_update_recs = df_temp[(df_temp['_merge'] == 'both') & (df_temp['modified_timestamp'] > df_temp['last_run_timestamp'])]
    df_delete_recs = df_temp[df_temp['_merge'] == 'right_only']

    # print(df_insert_recs.dtypes)

    # delete operation to delete any inactive job
    for _, row in df_delete_recs.iterrows():
        log.info(f"deleting job {row['job_name']}...")
        glue_instance.delete_glue_job(row['job_name'])
    
    # TODO: edge case: Check whether delete operator deletes a running instance of the job or not

    # create operation to create any new job that is inserted in Postgres db
    for _, row in df_insert_recs.iterrows():
        log.info(f"creating job {row['job_name']}...")
        glue_instance.create_glue_job(
            row['job_name'], 
            row['job_description'], 
            row['role_arn'], 
            row['script_location'],
            row['comman_name'], 
            row['max_concurrent_runs'],
            row['max_retries'], 
            row['timeout_minutes'], 
            row['max_capacity']
        )

    # update any existing job whose definition has been changed recently on Postgres db
    for _, row in df_update_recs.iterrows():
        log.info(f"updating job {row['job_name']}...")
        glue_instance.update_glue_job(
            row['job_name'], 
            row['job_description'], 
            row['role_arn'], 
            row['script_location'],
            row['comman_name'], 
            row['max_concurrent_runs'],
            row['max_retries'], 
            row['timeout_minutes'], 
            row['max_capacity']
        )

    postgres_instance.update_jobs_table()

    return


def main_admin(postgres_instance, glue_instance):
    sync_jobs(postgres_instance, glue_instance)


def main_user(args, postgres_instance, glue_instance):
    job_name = args.jobName
    job_instance = args.jobInstance

    # get job run id and status of previous job before running a new instance. This is to ensure
    # that previous job has completed.
    job_run_id, status = postgres_instance.get_job_status(job_name, job_instance)
    if status != 'completed':
        while True:
            job_status, err = glue_instance.get_glue_job_status(job_run_id)
            print(err)
            if err != None:
                log.error("job {job_name} with job run id {job_run_id} is failed with error {err}")
                break

            if job_status == 'SUCCEEDED':
                log.info("job {job_name} with job run id {job_run_id} is successfully completed")
                break
            time.sleep(20)
        # update control table with job status, either in-progress or completed
        postgres_instance.update_job_instance(job_name, job_instance, job_run_id, ctx=0)

    tables = postgres_instance.get_job_details(job_name, job_instance)
    job_run_id = glue_instance.start_glue_job(job_name, job_instance, tables)
    postgres_instance.update_job_instance(job_name, job_instance, job_run_id)

    while True:
        job_status, err = glue_instance.get_glue_job_status(job_run_id)
        # print(err)
        if err != None:
            log.error("job {job_name} with job run id {job_run_id} is failed with error {err}")
            break

        if job_status == 'SUCCEEDED':
            log.info("job {job_name} with job run id {job_run_id} is successfully completed")
            break
        time.sleep(20)

    postgres_instance.update_job_instance(job_name, job_instance, job_run_id, ctx=0)

    return

if __name__ == "__main__":
    set_logger()        # set logger for the app
    args = set_flag_parser()   # setup parser to parse named arguments

    postgres_instance = PostgresDBGlueService()
    glue_instance =  AwsGlueService()
    
    if args.userType == 'admin':
        main_admin(postgres_instance, glue_instance)
    elif args.userType == 'user':
        main_user(args, postgres_instance, glue_instance)
    else:
        raise ValueError("invalid --userType, type -h for help")

