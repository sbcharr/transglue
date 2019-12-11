from datetime import datetime
import logging as log
import psycopg2
import pandas.io.sql as sqlio
from abc import ABCMeta, abstractmethod
from aws.sql import sql_queries as sq
# from commons import commons as c


"""Base class for DB service."""


class MetadataDBService:
    """
    MetadataDBService is an abstract class which defines the various signatures for classes
    which implement these functions.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def __init__(self, host, dbname, port, user, password):
        self.__host = host
        self.__dbname = dbname
        self.__port = port
        self.__user = user
        self.__password = password

    @abstractmethod
    def create_db_conn(self):
        """
        creates a database object to the underlying db
        """
        raise NotImplementedError()

    @abstractmethod
    def get_glue_jobs_from_db(self):
        """
        this is responsible to get all related glue jobs from AWS Glue service.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_job_status(self, job_name, job_instance):
        """
        this function retrieves job status from metadata db.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_job_details(self, job_name, job_instance):
        """
        this function retrieves job details from metadata db.
        """
        raise NotImplementedError()

    @abstractmethod
    def update_jobs_table(self):
        """
        this function updates the control table called 'jobs' in metadata db.
        """
        raise NotImplementedError()

    @abstractmethod
    def update_job_instance(self, job_name, job_instance, job_run_id, job_status_ctx):
        """
        this function updates the control table called 'jobs_instances' in metadata db.
        """
        raise NotImplementedError()

    # @abstractmethod
    # def copy_to_database(self, target_table, temp_s3_bucket, iam_role):
    #     """
    #
    #     :return:
    #     """
    #     raise NotImplementedError()

    @abstractmethod
    def update_job_details(self, job_name, job_instance, table):
        """

        :return:
        """
        raise NotImplementedError()


class PostgresDBService(MetadataDBService):
    """
    PostgresDBService inherits  the abstract class MetadataDBService and implements its
    abstract methods.
    """
    def __init__(self, host, dbname, port, user, password):
        super().__init__(host, dbname, port, user, password)

    def create_db_conn(self):
        try:
            conn = psycopg2.connect("dbname={} host={} port={} user={} password={}".format(self.__dbname, self.__host,
                                                                                           self.__port, self.__user,
                                                                                           self.__password))
        except psycopg2.Error as e:
            log.error("could not make connection to the Postgres database")
            log.error(e)
            raise
        cur = conn.cursor()

        conn.set_session(autocommit=True)
        log.info("successfully created connection to the database, autocommit is on")

        return conn, cur

    def create_postgres_db_objects(self):
        """
        function to create the control table related db objects
        """
        conn, cur = self.create_db_conn()
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
        conn, cur = self.create_db_conn()
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

    def is_active_job(self, job_name, job_instance):
        conn, cur = self.create_db_conn()
        sql_stmt = sq.select_is_active_job.format(job_name, job_instance)

        try:
            cur.execute(sq.use_schema)
            df = sqlio.read_sql_query(sql_stmt, conn)
        except Exception as e:
            log.error(e)
            raise
        finally:
            cur.close()
            conn.close()
            log.info("successfully closed the db connection")

        return df

    def update_jobs_table(self):
        conn, cur = self.create_db_conn()

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

        conn, cur = self.create_db_conn()
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

        log.info("job_instances table is successfully updated")

    def update_job_details(self, job_name, job_instance, table):
        conn, cur = self.create_db_conn()

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
        conn, cur = self.create_db_conn()
        sql_stmt = sq.select_from_job_instances.format(job_name, job_instance)

        try:
            cur.execute(sq.use_schema)
            cur.execute(sql_stmt)
            row = cur.fetchone()
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
        conn, cur = self.create_db_conn()
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

    def truncate_stage_table(self, target_table):
        conn, cur = self.create_db_conn()
        sql_stmt = sq.truncate_table_stg.format(target_table)

        try:
            cur.execute(sql_stmt)
        except Exception as e:
            log.info("truncate stage table {}...".format(target_table))
            log.error(e)
            raise
        finally:
            cur.close()
            conn.close()
            log.info("successfully closed the db connection")

        log.info("successfully truncated the stage table {}".format(target_table))

    # def copy_to_database(self, target_table, temp_s3_bucket, iam_role):
    #     conn, cur = self.create_db_conn()
    #     s3_path = "s3://{}/data/{}/parquet/".format(temp_s3_bucket, target_table)
    #     sql_stmt = sq.load_data_to_table.format(target_table, s3_path, iam_role)
    #
    #     try:
    #         cur.execute(sql_stmt)
    #     except Exception as e:
    #         log.info("failed to load data to {} ...".format(target_table))
    #         log.error(e)
    #     finally:
    #         cur.close()
    #         conn.close()
    #         log.info("successfully closed the db connection")

