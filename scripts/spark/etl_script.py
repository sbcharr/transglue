import sys
# from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
# from pyspark.context import SparkContext
from awsglue.context import GlueContext
# from awsglue.job import Job
from pyspark.sql import SparkSession


def get_job_args():
    return getResolvedOptions(sys.argv, ['JOB_NAME', 'job-instance', 'tables'])

# def create_glue_context():
#     return GlueContext(SparkContext.getOrCreate())

def create_spark_session(app_name):
        spark = SparkSession.builder \
                .appName(app_name) \
                .getOrCreate()

        return spark

def create_glue_dynamic_frame(database, table_name, partition_predicate=None):
    glue_df = GlueContext.create_dynamic_frame.from_catalog(database = database, table_name = table_name, push_down_predicate = partition_predicate)

    return glue_df

def glue_transformations(spark, table):
    database = "nycitytaxi"
    max_load_date = spark.read.jdbc(...)
    partition_predicate = "load_date > '{}'".format(max_load_date)
    glue_df = create_glue_dynamic_frame(database, table, partition_predicate)

    spark_df = glue_df.toDF()
    if table == "yellow_taxi":
        pass
        

def main():
    args = get_job_args()
    spark = create_spark_session(args['app_name'])
    for table in args['tables']:
        glue_transformations(spark, table)
    

if __name__ == "__main__":
    main()

    





