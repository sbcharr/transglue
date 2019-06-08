import boto3
from botocore.exceptions import ClientError
import logging as log
import argparse, os, sys, time, datetime
import psycopg2
import pandas as pd
import pandas.io.sql as sqlio
from datetime import datetime
from abc import ABC, abstractmethod
from src.sql-queries import *


def flag_parser():
    """
    A function to parse parameterized input to command line arguments. It returns the object
    containing values of each input argument in a key/value fashion.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--jobName", help="job name to execute")
    parser.add_argument("--jobInstance", help="sequence number of job instance")
    parser.add_argument("--userType", help="user who runs this job, one of 'admin' or 'user'")
    parser.add_argument("--maxDpu", help="(optional) max dpu that AWS Glue uses, available only with user type 'user'")
    parser.add_argument("--logLevel", help="log level, values are 'debug', 'info', 'warning', 'error', 'critical'")

    args = parser.parse_args()

    return args

def set_logger(log_level):
    """
    Main logger set for the program. Default log level is set to INFO.
    """
    log_level_switcher = {
        'debug': log.DEBUG,
        'info': log.INFO,
        'warning': log.WARNING,
        'error': log.ERROR,
        'critical': log.CRITICAL
    }
    log.basicConfig(level=log_level_switcher.get(log_level, log.INFO))


class MetadataDBService(ABC):
    """
    MetadataDBService is an abstract class which defines the various signatures for classes
    which implements these function.
    """
    @abstractmethod
    def __create_db_conn(self, host, dbname, user, password):
        """
        creates a database objects to the underlying db
        """
        pass
    
    @abstractmethod
    def get_glue_jobs_from_db(self):
        """
        this is responsible to get all related glue jobs from AWS Glue service.
        """
        pass

    @abstractmethod
    def update_jobs_table(self):
        """
        this function updates the control table called 'jobs' in metadata db.
        """
        pass

    @abstractmethod
    def update_job_instance(self, job_name, job_instance, job_run_id, job_status_ctx):
        """
        this function updates the control table called 'jobs_instances' in metadata db.
        """
        pass

    @abstractmethod
    def get_job_status(self, job_name, job_instance):
        """
        this function retrives job status from metadata db.
        """
        pass

    @abstractmethod
    def get_job_details(self, job_name, job_instance):
        """
        this function retrives job details from metadata db.
        """
        pass



class PostgresDBService(MetadataDBService):
    """
    PostgresDBService inherits  the abstract class MetadataDBService and implements its
    abstract methods.
    """
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
        cur = conn.cursor()
        
        conn.set_session(autocommit=True)
        log.info("successfully created connection to Postgresql, autocommit is on")

        return conn, cur

    def create_postgres_db_objects(self):
        """
        function to create the control table related db objects
        """
        conn, cur = self.__create_db_conn(self.__host, self.__dbname, self.__user, self.__password)
        try:
            for query in create_queries:
                cur.execute(query)
        except Exception as err:
            log.info("error executing query {}".format(query))
            log.error(err)
            raise
        finally:
            cur.close()
            conn.close()
            log.info("successfully closed the db connection")

        log.info("sucessfully created all necessary control schema objects")

    

    def get_glue_jobs_from_db(self):
        """
        get all glue jobs from the 'jobs' table. This function return a pandas sql dataframe
        """
        conn = self.__create_db_conn(self.__host, self.__dbname, self.__user, self.__password)
        sql = "select * from jobs;"
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
        
        sql = f"update job_control.jobs set last_run_timestamp = '{value}'"
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

    def update_job_instance(self, job_name, job_instance, job_run_id, job_status_ctx=1):
        if job_status_ctx == 0:
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
        
        sql = f"select job_run_id, status from job_instances where job_name = '{job_name}' and job_instance = '{job_instance}'"
        try:
            cur.execute(sql)
            row = cur.fetchall()
        except Exception as err:
            log.info("select job_instances ...")
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
        
        sql = select_from_job_details.format(job_name, job_instance)
        try:
            cur.execute(sql)
            rows = cur.fetchall()
        except Exception as err:
            log.info("select job_instances ...")
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


class GlueJobService(AwsGlueService):
    def __init__(self):
        AwsGlueService.__init__(self)
    
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


    def start_glue_job(self, job_name, job_instance, tables, max_dpu=None):
        if max_dpu is not None:
            try:
                r = self.__client.start_job_run(
                    JobName=job_name,
                    Arguments={
                        '--job-instance': job_instance,
                        '--tables': tables
                    },
                    MaxCapacity=max_dpu
                )
            except Exception as err:
                log.error(err)
                raise
        else:
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

        log.info(f"glue job {job_name} is successfully started with job id {r['JobRunId']}")

        return r['JobRunId']

    def get_glue_job_status(self, job_name, job_run_id):
        try:
            r = self.__client.get_job_run(JobName=job_name, RunId=job_run_id, PredecessorsIncluded=False)
        except Exception as err:
            log.error(err)
            raise

        return r['JobRun']['JobRunState'], r['JobRun']['ErrorMessage']


class GlueCrawlerService(AwsGlueService):
    def __init__(self):
        AwsGlueService.__init__(self)

    def create_crawler_service(self, name, role, catalog_db_name, s3_target_path, description=None, table_prefix=None):
        try:
            # if  not self.is_crawler_available(name):
            _ = self.__client.create_crawler(
                Name=name,
                Role=role,
                DatabaseName=catalog_db_name,
                Description=description,
                Targets={
                    'S3Targets': [
                        {
                            'Path': s3_target_path
                        }
                    ]
                },
                SchemaChangePolicy={
                    'UpdateBehavior': 'LOG',
                    'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
                }
            )
            # else:
            #    log.info(f"crawler {name} is already available")
        except ClientError as e:
            if e.response['Error']['Code'] == "AlreadyExistsException":
                log.info(f"crawler {name} already exists")
            else:
                log.error(f"error: {e.response['Error']['Message']}")
                raise
    
    def start_glue_crawler(self, name):
        try:
            _ = self.__client.start_crawler(
                Name=name
            )
        except ClientError as e:
            if e.response['Error']['Code'] == "CrawlerRunningException":
                log.info(f"crawler {name} is already running, please run at a later time")
            else:
                log.error(f"error: {e.response['Error']['Message']}")
        
    def delete_glue_crawler(self, name):
        try:
            _ = self.__client.delete_crawler(
                    Name=name
                )
        except ClientError as e:
            if e.response['Error']['Code'] == "CrawlerRunningException":
                log.error(f"crawler {name} is currently running, try again later")
                raise
            else:
                log.error(f"error: {e.response['Error']['Message']}")
                raise

class GlueCatalogService(AwsGlueService):
    def __init__(self):
        AwsGlueService.__init__(self)

    def create_database_in_catalog(self, name, catalog_id=None, description=None, location_uri=None):
        try:  
            _ = self.__client.create_database(
                CatalogId=catalog_id,
                DatabaseInput={
                    'Name': name,
                    'Description': description,
                    'LocationUri': location_uri
                }
            )
        except ClientError as e:
            if e.response['Error']['Code'] == "AlreadyExistsException":
                log.info(f"database {name} already exists in catalog {catalog_id}")
        except ClientError as e:
            log.error(f"error: {e.response['Error']['Message']}")
            raise


def sync_jobs(postgres_instance, glue_instance):
    '''sync_job runs as a separate job on a periodic basic to sync up job info between Postgres
    db and AWS Glue. It receives a postgres instance and a glue instance as its parameters. Sync
    job should be executed by an Admin user.
    '''

    # Get all glue jobs from Postgres db
    df_jobs_all = postgres_instance.get_glue_jobs_from_db()  

    # get_glue_jobs retuns all job names from AWS Glue as a list
    df_glue_jobs = pd.DataFrame(glue_instance.get_glue_jobs(), columns=['job_name'])  
    
    # pandas dataframe is used to identify jobs that are to be created, updated and deleted in AWS Glue Service
    df_temp = pd.merge(df_jobs_all, df_glue_jobs, left_on='job_name', right_on='job_name', how='outer', indicator=True)
    
    df_insert_recs = df_temp[df_temp['_merge'] == 'left_only' & (df_temp['is_active'] == 'Y')]
    df_update_recs = df_temp[(df_temp['_merge'] == 'both') & (df_temp['is_active'] != 'Y') & (df_temp['modified_timestamp'] > df_temp['last_run_timestamp'])]
    df_delete_recs = df_temp[df_temp['_merge'] == 'left_only' & (df_temp['is_active'] != 'Y')]

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

    # update any existing job whose definition has been changed recently in Postgres db
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

    log.info("successfully synchronizd jobs between database and AWS Glue at {}".format(datetime.datetime.now()))


def main_admin(max_dpu, postgres_instance, glue_instance):
    postgres_instance.create_postgres_db_objects()
    sync_jobs(postgres_instance, glue_instance)


def main_user(args, postgres_instance, glue_instance):
    job_name = args.jobName
    job_instance = args.jobInstance
    max_dpu = args.maxDpu

    # get job run id and status of previous job before running a new instance. This is to ensure
    # that previous job has completed.
    job_run_id, status = postgres_instance.get_job_status(job_name, job_instance)
    if status != 'completed':
        while True:
            job_status, err = glue_instance.get_glue_job_status(job_run_id)
            print(err)
            if err != None:
                log.error(f"job {job_name} with job run id {job_run_id} is failed with error {err}")
                break

            if job_status == 'SUCCEEDED':
                log.info(f"job {job_name} with job run id {job_run_id} is successfully completed")
                break
            time.sleep(20)
        # update control table with job status, either in-progress or completed
        postgres_instance.update_job_instance(job_name, job_instance, job_run_id, ctx=0)

    tables = postgres_instance.get_job_details(job_name, job_instance)
    job_run_id = glue_instance.start_glue_job(job_name, job_instance, max_dpu, tables)
    postgres_instance.update_job_instance(job_name, job_instance, job_run_id)

    while True:
        job_status, err = glue_instance.get_glue_job_status(job_run_id)
        # print(err)
        if err != None:
            log.error(f"job {job_name} with job run id {job_run_id} is failed with error {err}")
            break

        if job_status == 'SUCCEEDED':
            log.info(f"job {job_name} with job run id {job_run_id} is successfully completed")
            break
        time.sleep(20)

    postgres_instance.update_job_instance(job_name, job_instance, job_run_id, job_status_ctx=0)

    return

def main():
    args = flag_parser()        # setup parser to parse named arguments
    set_logger(args.logLevel)       # set logger for the app

    postgres_instance = PostgresDBService()
    glue_instance =  GlueJobService()
    
    if args.userType == 'admin':
        main_admin(args.maxDpu, postgres_instance, glue_instance)
    elif args.userType == 'user':
        main_user(args, postgres_instance, glue_instance)
    else:
        raise ValueError("invalid --userType, type -h for help")



if __name__ == "__main__":
    main()
