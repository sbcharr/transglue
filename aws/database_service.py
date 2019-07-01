from datetime import datetime
import logging as log
import psycopg2
import pandas.io.sql as sqlio
import abc
from aws.sql import sql_queries as sq
from commons import commons as c


class MetadataDBService:
    """
    MetadataDBService is an abstract class which defines the various signatures for classes
    which implements these function.
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def create_db_conn(self, host, dbname, user, password):
        """
        creates a database objects to the underlying db
        """
        pass

    @abc.abstractmethod
    def get_glue_jobs_from_db(self):
        """
        this is responsible to get all related glue jobs from AWS Glue service.
        """
        pass

    @abc.abstractmethod
    def get_job_status(self, job_name, job_instance):
        """
        this function retrieves job status from metadata db.
        """
        pass

    @abc.abstractmethod
    def get_job_details(self, job_name, job_instance):
        """
        this function retrieves job details from metadata db.
        """
        pass

    @abc.abstractmethod
    def update_jobs_table(self):
        """
        this function updates the control table called 'jobs' in metadata db.
        """
        pass

    @abc.abstractmethod
    def update_job_instance(self, job_name, job_instance, job_run_id, job_status_ctx):
        """
        this function updates the control table called 'jobs_instances' in metadata db.
        """
        pass

    @abc.abstractmethod
    def copy_to_database(self, target_table, temp_s3_bucket, iam_role):
        """

        :return:
        """
        pass

    @abc.abstractmethod
    def update_job_details(self, job_name, job_instance, table):
        """

        :return:
        """
        pass


class PostgresDBService(MetadataDBService):
    """
    PostgresDBService inherits  the abstract class MetadataDBService and implements its
    abstract methods.
    """
    def __init__(self):
        self.__host = c.os.environ['GLUE_DB_HOST']
        self.__dbname = c.os.environ['GLUE_JOBS_DB']
        self.__user = c.os.environ['GLUE_DB_USER']
        self.__password = c.os.environ['GLUE_DB_PASSWORD']

    def create_db_conn(self, host, dbname, user, password):
        try:
            conn = psycopg2.connect("host={} dbname={} user={} password={}".format(host, dbname, user, password))
        except psycopg2.Error as e:
            log.info("Error: Could not make connection to the Postgres database")
            log.error(e)
            raise
        cur = conn.cursor()

        conn.set_session(autocommit=True)
        log.info("successfully created connection to Postgresql, autocommit is on")

        return conn, cur

    def create_postgres_db_objects(self):
        """
        function to create the control table related db objects
        """
        conn, cur = self.create_db_conn(self.__host, self.__dbname, self.__user, self.__password)
        try:
            for sql_stmt in sq.create_sql_stmts:
                cur.execute(sql_stmt)
        except Exception as e:
            log.info("error executing sql query")
            log.error(e)
            raise
        finally:
            cur.close()
            conn.close()
            log.info("successfully closed the db connection")

        log.info("successfully created all necessary control schema objects")

    def get_glue_jobs_from_db(self):
        """
        get all glue jobs from the 'jobs' table. This function returns a pandas sql data frame
        """
        conn, cur = self.create_db_conn(self.__host, self.__dbname, self.__user, self.__password)
        sql_stmt = sq.select_from_jobs

        try:
            cur.execute(sq.use_schema)
            df = sqlio.read_sql_query(sql_stmt, conn)
        except Exception as e:
            log.info("Error: select *")
            log.error(e)
            raise
        finally:
            cur.close()
            conn.close()
            log.info("successfully closed the db connection")

        log.info("successfully executed function 'get_glue_jobs_from_db'")

        return df

    # TODO: create a separate variadic function encapsulating db operating to avoid repetitiveness

    def update_jobs_table(self):
        conn, cur = self.create_db_conn(self.__host, self.__dbname, self.__user, self.__password)

        current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sql_stmt = sq.update_table_jobs.format(current_timestamp)

        try:
            cur.execute(sq.use_schema)
            cur.execute(sql_stmt)
        except Exception as e:
            log.info("update jobs ...")
            log.error(e)
            raise
        finally:
            cur.close()
            conn.close()
            log.info("successfully closed the db connection")

        log.info("column 'last_sync_timestamp' in 'jobs' table is successfully updated")

    def update_job_instance(self, job_name, job_instance, job_run_id, status):
        # if job_status_ctx == 0:
        #     status = "completed"
        # else:
        #     status = "in-progress"

        conn, cur = self.create_db_conn(self.__host, self.__dbname, self.__user, self.__password)
        sql_stmt = sq.update_table_job_instances.format(job_run_id, status, job_name, job_instance)

        try:
            cur.execute(sq.use_schema)
            cur.execute(sql_stmt)
        except Exception as e:
            log.info("update job_instances ...")
            log.error(e)
            raise
        finally:
            cur.close()
            conn.close()
            log.info("successfully closed the db connection")

        log.info("column '{}' in '{}' table is successfully updated".format(job_run_id, job_instance))

    def update_job_details(self, job_name, job_instance, table):
        conn, cur = self.create_db_conn(self.__host, self.__dbname, self.__user, self.__password)

        current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sql_stmt = sq.update_table_job_details.format(current_timestamp, job_name,
                                                      job_instance, table)   # table = schema.table_name

        try:
            cur.execute(sq.use_schema)
            cur.execute(sql_stmt)
        except Exception as e:
            log.info("update job_details ...")
            log.error(e)
            raise
        finally:
            cur.close()
            conn.close()
            log.info("successfully closed the db connection")

        log.info("column 'job_run_id' in 'job_details' table is successfully updated")

    def get_job_status(self, job_name, job_instance):
        conn, cur = self.create_db_conn(self.__host, self.__dbname, self.__user, self.__password)
        sql_stmt = sq.select_from_job_instances.format(job_name, job_instance)

        try:
            cur.execute(sq.use_schema)
            cur.execute(sql_stmt)
            row = cur.fetchall()
        except Exception as e:
            log.info("select job_instances ...")
            log.error(e)
            raise
        finally:
            cur.close()
            conn.close()
            log.info("successfully closed the db connection")

        return row[0], row[1]

    def get_job_details(self, job_name, job_instance):
        conn, cur = self.create_db_conn(self.__host, self.__dbname, self.__user, self.__password)
        sql_stmt = sq.select_from_job_details.format(job_name, job_instance)

        try:
            cur.execute(sq.use_schema)
            cur.execute(sql_stmt)
            rows = cur.fetchall()
        except Exception as e:
            log.info("select job_instances ...")
            log.error(e)
            raise
        finally:
            cur.close()
            conn.close()
            log.info("successfully closed the db connection")

        tables = []
        for row in rows:
            tables.append(row[0])

        return tables

    def copy_to_database(self, target_table, temp_s3_bucket, iam_role):
        conn, cur = self.create_db_conn(self.__host, self.__dbname, self.__user, self.__password)
        s3_path = "s3://{}/data/{}/parquet/".format(temp_s3_bucket, target_table)
        sql_stmt = sq.load_data_to_table.format(target_table, s3_path, iam_role)

        try:
            cur.execute(sql_stmt)
        except Exception as e:
            log.info("failed to load data to {} ...".format(target_table))
            log.error(e)
        finally:
            cur.close()
            conn.close()
            log.info("successfully closed the db connection")
