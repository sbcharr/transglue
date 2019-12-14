import logging as log
from commons import commons as c
from service import sqs_service


def create_sqs_queue(job_name, job_instance, sqs_instance):
    # For SQS FIFO
    queue_name = job_name + "_" + job_instance + ".fifo"
    queue_url = sqs_instance.get_queue_url(queue_name)

    if queue_url:
        sqs_queue_url = queue_url['QueueUrl']

        # deletes the existing queue
        sqs_instance.delete_fifo_queue(sqs_queue_url)
        log.info("deleted fifo queue {}".format(queue_name))

    else:
        log.error("no such job name or job instance found, please check")
        raise


def main():
    # TODO: input validation
    args = c.setup()

    job_name = args.jobName
    job_instance = args.jobInstance

    sqs_instance = sqs_service.AwsSqsService()

    if args.userType != 'user':
        log.error("only valid option is user; invalid --userType, type -h for help")
        raise

    create_sqs_queue(job_name, job_instance, sqs_instance)


if __name__ == "__main__":
    main()
