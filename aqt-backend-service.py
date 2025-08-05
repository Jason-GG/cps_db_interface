from cps_db_pg_interface import create_model_class, createTables, logger
from cps_db_pg_interface  import create_data_table_class
from botocore.exceptions import ClientError
import json
import boto3
import time
import traceback
import datetime

g_dbDict = {
    "DB_DB": "aqt_info_db",
    "DB_HOST": "aqt-info-db.c2l1ffm73dnf.us-east-1.rds.amazonaws.com",
    "DB_USER": "aqt_user",
    "DB_PASSWORD": "CoalescenceQWER!#$%0.",
    "DB_CONNECTOR": "psycopg2",
    "DB_SSL_MODE": "disable"
}


def custom_get_update_judge(cls, args=None):
    return getattr(cls, "customer_id") == args["customer_id"]


def create_azure_table_model():
    tablename = "azure_vm_table"
    schema = [
        {"name": "id", "type": "Integer", "primary_key": True, "autoincrement": True},
        {"name": "customer_id", "type": "String", "length": 50, "nullable": True, "unique": True},
        {"name": "client", "type": "String", "length": 100, "nullable": True},
        {"name": "subscription_id", "type": "String", "length": 100, "nullable": True},
        {"name": "environments", "type": "JSON", "nullable": True},

    ]
    AzureModel = create_model_class(tablename, schema)
    AzureModel.get_update_judge = classmethod(custom_get_update_judge)
    return AzureModel


def create_aws_table_model():
    tablename = "aws_vm_table"
    schema = [
        {"name": "id", "type": "Integer", "primary_key": True, "autoincrement": True},
        {"name": "customer_id", "type": "String", "length": 50, "nullable": True, "unique": True},
        {"name": "client", "type": "String", "length": 100, "nullable": True},
        {"name": "account_id", "type": "String", "length": 50, "nullable": True},
        {"name": "environments", "type": "JSON", "nullable": True},
        {"name": "natgatewayList", "type": "JSON", "nullable": True},
        {"name": "route53List", "type": "JSON", "nullable": True},
        {"name": "s3List", "type": "JSON", "nullable": True},

    ]
    AwsModel = create_model_class(tablename, schema)
    AwsModel.get_update_judge = classmethod(custom_get_update_judge)
    return AwsModel

g_AzureModel = create_azure_table_model()
g_AwsModel = create_aws_table_model()

def create_test_table(Model):
    # usage of createTables
    createTables(g_dbDict)
    logger.info(Model.__table__)
    logger.info(Model.__dict__)
    logger.info("Test table created successfully.")


def build_table_func():
    create_test_table(g_AzureModel)
    create_test_table(g_AwsModel)


def aws_ingest_data_func(args):
    AwsModelHandler = create_data_table_class("AwsModel", g_AwsModel)
    handler_instance = AwsModelHandler(g_dbDict)
    res = handler_instance.data_update(args=args)
    logger.info(res)


def azure_ingest_data_func(args):
    AzureModelHandler = create_data_table_class("AzureModel", g_AzureModel)
    handler_instance = AzureModelHandler(g_dbDict)
    res = handler_instance.data_update(args=args)
    logger.info(res)


class SqsClass:
    """Encapsulates Amazon SQS queue operations."""

    QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/570188313908/aqt-db-queue.fifo'
    # session = boto3.Session(profile_name="default")
    # s3 = session.client('s3')
    def __init__(self, profile_name="default", queue_url=None) -> None:
        session = boto3.Session(profile_name=profile_name)
        self.sqs_client = session.client('sqs')
        self.sqs_resource = session.resource('sqs')
        self.queue_url = queue_url or self.QUEUE_URL
        self.queue = self.sqs_resource.Queue(self.queue_url)


    def receive_messages(self, wait_time=0, max_messages=1):
        """
        Receives messages from the configured SQS queue.
        :param wait_time: Number of seconds to wait for a message (long polling).
        :param max_messages: Maximum number of messages to retrieve.
        :return: List of messages or an empty list.
        """
        try:
            response = self.sqs_client.receive_message(
                QueueUrl=self.queue_url,
                WaitTimeSeconds=wait_time,
                MaxNumberOfMessages=max_messages
            )
            messages = response.get("Messages", [])
            # logger.info(f"Received {len(messages)} message(s).")
            return messages
        except ClientError as e:
            logger.exception("Failed to receive messages.")
            raise e


def queue_receive_messages():
    """
    Function to continuously receive messages from the SQS queue and process them.
    Wrapped in a try/except block to ensure it keeps running on errors.
    """
    sqs = SqsClass(queue_url="https://sqs.us-east-1.amazonaws.com/570188313908/aqt-db-queue.fifo")
    logger.info("Starting to receive messages from the SQS queue...")

    while True:
        try:
            messages = sqs.receive_messages(wait_time=0, max_messages=1)
            if messages:
                for message in messages:
                    logger.info(f"Message ID: {message['MessageId']}")
                    logger.info(f"Message Body: {message['Body']}")
                    logger.info(f"Message Body type: {type(message['Body'])}")
                    # logger.info(f"Message Body: {message['MessageGroupId']}")
                    body_json = json.loads(message['Body'])
                    try:
                        if body_json["type"] == "azure":
                            azureData = body_json["data"]
                            logger.info(f"Processing Azure data")
                            del azureData["authorization_source"]
                            azure_ingest_data_func(azureData)
                        elif body_json["type"] == "aws":
                            awsData = body_json["data"]
                            logger.info(f"Processing AWS data")
                            aws_ingest_data_func(awsData)
                        # Delete the message after successful processing
                        sqs.sqs_client.delete_message(
                            QueueUrl=sqs.queue_url,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                        logger.info("Message deleted successfully.")

                    except Exception as e:
                        # Delete the message after successful processing
                        sqs.sqs_client.delete_message(
                            QueueUrl=sqs.queue_url,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                        logger.info("Message deleted successfully.")
                        logger.error(f"Error processing message: {e}")
                        logger.error(traceback.format_exc())

            else:
                time.sleep(1)  # No messages, wait briefly before polling again

        except Exception as e:
            logger.error(f"Error receiving messages: {e}")
            logger.error(traceback.format_exc())
            time.sleep(2)  # Sleep before retrying to avoid tight error loop


if __name__ == "__main__":
    # Create the test tables for Azure and AWS
    # build_table_func()

    # ingest data into the AWS table
    # aws_ingest_data_func(awsData)

    # ingest data into the Azure table
    # del azureData["authorization_source"]
    # azure_ingest_data_func(azureData)
    queue_receive_messages()
