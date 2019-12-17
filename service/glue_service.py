import boto3
from botocore.exceptions import ClientError
# import logging as log
from commons import commons

CONFIG = commons.get_config()
log = commons.get_logger(CONFIG['logLevel'], CONFIG['logFile'])

# All functionality is based on AWS Glue boto3 API version 1.10.38


class AwsGlueService:
    def __init__(self, region_name, aws_access_key_id=None, aws_secret_access_key=None):
        self.client = boto3.client('glue', region_name=region_name, aws_access_key_id=aws_access_key_id,
                                   aws_secret_access_key=aws_secret_access_key)


class GlueJobService(AwsGlueService):
    def __init__(self, region_name, aws_access_key_id, aws_secret_access_key):
        super().__init__(region_name, aws_access_key_id, aws_secret_access_key)
    
    def get_glue_jobs(self):
        is_run = 0
        next_token = ""
        jobs = []
        try:
            while is_run == 0:
                response = self.client.get_jobs(NextToken=next_token, MaxResults=1000,)
                for job in response['Jobs']:
                    jobs.append(job['Name'])
                if 'NextToken' in response:
                    next_token = response['NextToken']
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
                        glue_version,
                        max_capacity,
                        command_name='glueetl',
                        python_version="3",
                        max_concurrent_runs=10,
                        max_retries=1, 
                        timeout=180):
        try:
            response = self.client.create_job(
                Name=job_name,
                Description=description,
                Role=role,
                ExecutionProperty={
                    'MaxConcurrentRuns': int(max_concurrent_runs),
                },
                Command={'Name': command_name,
                         'ScriptLocation': script_loc,
                         'PythonVersion': str(python_version),
                         },
                MaxRetries=int(max_retries),
                Timeout=int(timeout),
                MaxCapacity=float(max_capacity),
                GlueVersion=glue_version,
            )
        except ClientError as e:
            log.error(e)
            raise

        log.info("aws job {} is successfully created".format(response['Name']))

    def update_glue_job(self,     
                        job_name,
                        description, 
                        role, 
                        script_loc,
                        log_uri,
                        max_capacity,
                        command_name='glueetl', 
                        max_concurrent_runs=3, 
                        max_retries=1, 
                        timeout=180):
        try:
            response = self.client.update_job(
                JobName=job_name,
                JobUpdate={
                    'Description': description,
                    'LogUri': log_uri,
                    'Role': role,
                    'ExecutionProperty': {
                        'MaxConcurrentRuns': int(max_concurrent_runs),
                    },
                    'Command': {
                        'Name': command_name,
                        'ScriptLocation': script_loc,
                    },
                    'MaxRetries': int(max_retries),
                    'Timeout': int(timeout),
                    'MaxCapacity': float(max_capacity),
                }
            )
        except ClientError as e:
            log.error(e)
            raise

        log.info("aws job {} is successfully updated".format(response['JobName']))

    def delete_glue_job(self, job_name):
        # print(job_name)
        try:
            response = self.client.delete_job(
                JobName=job_name,
            )
        except ClientError as e:
            log.error(e)
            raise

        # print(response['JobName'])

        log.info("successfully deleted the aws job {}".format(response['JobName']))

    def start_glue_job(self, job_name, job_instance, tables, max_dpu=None, from_date="", to_date=""):
        if max_dpu is not None:
            try:
                response = self.client.start_job_run(
                    JobName=job_name,
                    Arguments={
                        '--job-instance': job_instance,
                        '--tables': tables,
                        '--from-date': from_date,
                        '--to-date': to_date,
                    },
                    MaxCapacity=float(max_dpu)
                )
            except ClientError as e:
                log.error(e)
                raise
        else:
            try:
                response = self.client.start_job_run(
                    JobName=job_name,
                    Arguments={
                        '--job-instance': job_instance,
                        '--tables': tables
                    }
                )
            except ClientError as e:
                log.error(e)
                raise

        log.info("aws job {} is successfully started with job id {}".format(job_name, response['JobRunId']))

        return response['JobRunId']

    def get_glue_job_status(self, job_name, job_run_id):
        try:
            response = self.client.get_job_run(JobName=job_name, RunId=job_run_id, PredecessorsIncluded=False,)
        except ClientError as e:
            log.error(e)
            raise
        if 'ErrorMessage' in response['JobRun'].keys():
            log.info("job {} with job run id {} is failed with error {}".format(job_name, job_run_id,
                                                                                response['JobRun']['ErrorMessage']))

        return response['JobRun']['JobRunState']


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
#                 log.info(f"metadata {name} already exists in catalog {catalog_id}")
#         except ClientError as e:
#             log.error(f"error: {e.response['Error']['Message']}")
#             raise


