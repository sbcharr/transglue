import boto3
from botocore.exceptions import ClientError
import logging as log
from commons import commons as c


class AwsGlueService:
    def __init__(self):
        self.client = boto3.client('glue', region_name=c.os.environ['REGION_NAME'],
                                   aws_access_key_id=c.os.environ['AWS_ACCESS_KEY_ID'],
                                   aws_secret_access_key=c.os.environ['AWS_SECRET_ACCESS_KEY'])


class GlueJobService(AwsGlueService):
    def __init__(self):
        AwsGlueService.__init__(self)
    
    def get_glue_jobs(self):
        is_run = 0
        next_token = ""
        jobs = []
        try:
            while is_run == 0:
                r = self.client.get_jobs(NextToken=next_token, MaxResults=100)
                for job in r['Jobs']:
                    jobs.append(job['Name'])
                if 'NextToken' in r:
                    next_token = r['NextToken']
                else:
                    is_run = 1
        except ClientError as e:
            log.error(e)
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
            _ = self.client.create_job(
                Name=job_name,
                Description=description,
                Role=role,
                ExecutionProperty={
                    'MaxConcurrentRuns': int(max_concurrent_runs),
                },
                Command={'Name': command_name,
                         'ScriptLocation': script_loc
                        },
                MaxRetries=int(max_retries),
                Timeout=int(timeout),
                AllocatedCapacity=int(max_capacity)
            )
        except Exception as err:
            log.error(err)
            raise

        log.info("aws job {} is successfully created".format(job_name))

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
            _ = self.client.update_job(
                JobName=job_name,
                JobUpdate={
                    'Description': description,
                    'Role': role,
                    'ExecutionProperty': {
                        'MaxConcurrentRuns': int(max_concurrent_runs)
                    },
                    'Command': {
                        'Name': command_name,
                        'ScriptLocation': script_loc
                    },
                    'MaxRetries': int(max_retries),
                    'Timeout': int(timeout),
                    'AllocatedCapacity': int(max_capacity)
                }
            )
        except ClientError as e:
            log.error(e)
            raise

        log.info("aws job {} is successfully updated".format(job_name))

    def delete_glue_job(self, job_name):
        # print(job_name)
        try:
            r = self.client.delete_job(
                JobName=job_name
            )
        except ClientError as e:
            log.error(e)
            raise

        print(r['JobName'])

        log.info("aws job {} is successfully deleted".format(job_name))

    def start_glue_job(self, job_name, job_instance, tables, max_dpu=None):
        if max_dpu is not None:
            try:
                r = self.client.start_job_run(
                    JobName=job_name,
                    Arguments={
                        '--job-instance': job_instance,
                        '--tables': tables
                    },
                    MaxCapacity=max_dpu
                )
            except ClientError as e:
                log.error(e)
                raise
        else:
            try:
                r = self.client.start_job_run(
                    JobName=job_name,
                    Arguments={
                        '--job-instance': job_instance,
                        '--tables': tables
                    }
                )
            except ClientError as e:
                log.error(e)
                raise

        log.info("aws job {} is successfully started with job id {}".format(job_name, r['JobRunId']))

        return r['JobRunId']

    def get_glue_job_status(self, job_name, job_run_id):
        try:
            r = self.client.get_job_run(JobName=job_name, RunId=job_run_id, PredecessorsIncluded=False)
        except ClientError as e:
            log.error("job {} with job run id {} is failed with error {}".format(job_name,
                                                                                 job_run_id,
                                                                                 e.response['Error']['Message']))
            raise

        return r['JobRun']['JobRunState']


# class GlueCrawlerService(AwsGlueService):
#     def __init__(self):
#         AwsGlueService.__init__(self)

#     def create_crawler_service(self, name, role, catalog_db_name, s3_target_path,
#                                description=None, table_prefix=None):
#         try:
#             # if  not self.is_crawler_available(name):
#             _ = self.client.create_crawler(
#                 Name=name,
#                 Role=role,
#                 DatabaseName=catalog_db_name,
#                 Description=description,
#                 Targets={
#                     'S3Targets': [
#                         {
#                             'Path': s3_target_path
#                         }
#                     ]
#                 },
#                 SchemaChangePolicy={
#                     'UpdateBehavior': 'LOG',
#                     'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
#                 }
#             )
#             # else:
#             #    log.info(f"crawler {name} is already available")
#         except ClientError as e:
#             if e.response['Error']['Code'] == "AlreadyExistsException":
#                 log.info(f"crawler {name} already exists")
#             else:
#                 log.error(f"error: {e.response['Error']['Message']}")
#                 raise
    
#     def start_glue_crawler(self, name):
#         try:
#             _ = self.client.start_crawler(
#                 Name=name
#             )
#         except ClientError as e:
#             if e.response['Error']['Code'] == "CrawlerRunningException":
#                 log.info(f"crawler {name} is already running, please run at a later time")
#             else:
#                 log.error(f"error: {e.response['Error']['Message']}")
        
#     def delete_glue_crawler(self, name):
#         try:
#             _ = self.client.delete_crawler(
#                     Name=name
#                 )
#         except ClientError as e:
#             if e.response['Error']['Code'] == "CrawlerRunningException":
#                 log.error(f"crawler {name} is currently running, try again later")
#                 raise
#             else:
#                 log.error(f"error: {e.response['Error']['Message']}")
#                 raise

# class GlueCatalogService(AwsGlueService):
#     def __init__(self):
#         AwsGlueService.__init__(self)

#     def create_database_in_catalog(self, name, catalog_id=None, description=None, location_uri=None):
#         try:  
#             _ = self.client.create_database(
#                 CatalogId=catalog_id,
#                 DatabaseInput={
#                     'Name': name,
#                     'Description': description,
#                     'LocationUri': location_uri
#                 }
#             )
#         except ClientError as e:
#             if e.response['Error']['Code'] == "AlreadyExistsException":
#                 log.info(f"database {name} already exists in catalog {catalog_id}")
#         except ClientError as e:
#             log.error(f"error: {e.response['Error']['Message']}")
#             raise


