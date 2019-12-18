import sys
from awsglue.utils import getResolvedOptions
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


# def create_glue_dynamic_frame(database, table_name, partition_predicate=None):
#     glue_df = GlueContext.create_dynamic_frame.from_catalog(database=database, table_name=table_name,
#                                                             push_down_predicate=partition_predicate)
#
#     return glue_df


def glue_transformations(spark, input_data, output_dir, out_format):
    df = spark.read.option("header", True).option("inferSchema", True).csv(input_data)
    df.write.save(path=output_dir, format=out_format, compression="snappy", mode="overwrite")


def main():
    args = get_job_args()
    spark = create_spark_session(args['app_name'])
    out_format = "parquet"

    for table in args['tables']:
        input_data = "s3a://sb-movielens-mumbai/movielens-db/{}/*.csv".format(table)
        output_dir = "s3a://sb-movielens-mumbai/out/movie_lens/{}/".format(table)
        glue_transformations(spark, input_data, output_dir, out_format)


if __name__ == "__main__":
    main()
