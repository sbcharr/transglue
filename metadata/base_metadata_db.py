from abc import ABCMeta, abstractmethod


"""Base class for DB service."""


class MetadataService:
    """
    MetadataService is an abstract class which defines the various signatures for classes
    which implement these functions.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def __init__(self, host, dbname, port, user, password):
        self.host = host
        self.dbname = dbname
        self.port = port
        self.user = user
        self.password = password

    @abstractmethod
    def create_db_conn(self):
        """
        creates a metadata object to the underlying db
        """
        raise NotImplementedError

    @abstractmethod
    def get_glue_jobs_from_db(self):
        """
        this is responsible to get all related glue jobs from the AWS Glue service.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_job_status(self, job_name, job_instance):
        """
        this function retrieves job status from the metadata db.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_job_details(self, job_name, job_instance):
        """
        this function retrieves job details of active tables from the metadata db.
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
        this function updates the control table called 'jobs_instances' in the metadata db.
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
