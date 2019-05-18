import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


def get_job_args():
    return getResolvedOptions(sys.argv, ['JOB_NAME', 'job-instance', 'tables'])

def create_glue_context():
    return GlueContext(SparkContext.getOrCreate())

def create_glue_dynamic_frame(glue_context, database, table_name, partition_predicate=None):
    ddf = glue_context.create_dynamic_frame.from_catalog(database = database,
                                                    table_name = table_name,
                                            push_down_predicate = partition_predicate)

    return ddf


def main():
    args = get_job_args()
    

if __name__ == "__main__":
    main()

    





