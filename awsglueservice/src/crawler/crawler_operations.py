import boto3

class AWSGlueCrawler:
    def __init__(self):
        self.__client = boto3.client("glue")

    
