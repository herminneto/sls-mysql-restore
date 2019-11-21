import os
import json
import zipfile
import urllib.request
import tempfile
import boto3
from botocore.exceptions import ClientError


def api_trigger(event, context):
    """
    :param event:
    :param context:
    :return:
    """
    sqs_name = os.environ["TRIGGER_SQS_NAME"]
    try:
        sqs = boto3.resource('sqs')
        queue = sqs.get_queue_by_name(QueueName=sqs_name)
        response = queue.send_message(MessageBody=json.dumps({'data': 'start'}))
    except ClientError as e:
        response = e

    return {
        'status': 200,
        'response': response
    }


def sqs_trigger(event, context):
    """
    :param event:
    :param context:
    :return:
    """
    print(event)
    message = json.loads(event['Records'][0]['body'])
    if message['data'] != 'start':
        print("Unknown message")
        return

    sqs_name = os.environ["TRIGGER_SQS_NAME"]
    try:
        sqs_client = boto3.client('sqs')
        response = sqs_client.get_queue_url(QueueName=sqs_name)
        response = sqs_client.delete_message(
            QueueUrl=response['QueueUrl'],
            ReceiptHandle=event['Records'][0]['receiptHandle']
        )
        print(response)
    except ClientError as e:
        print(e)
        return

    rds_client = boto3.client('rds-data')

    db_arn = os.environ["DB_ARN"]
    db_secret_arn = os.environ["DB_SECRET_ARN"]
    db_name = os.environ["DB_NAME"]

    download_path = "/tmp/download.zip"
    ret = urllib.request.urlretrieve("https://www.onetcenter.org/dl_files/database/db_24_0_mysql.zip", download_path)

    with tempfile.TemporaryDirectory() as tmpdir:
        os.chdir(tmpdir)

        with zipfile.ZipFile(download_path, 'r') as archive:
            archive.extractall()
        files_path = tmpdir + '/db_24_0_mysql'
        files = os.listdir(files_path)
        files.remove('Read Me.txt')
        files.sort()

        for item in files:
            table_name = item[3:-4]
            print('Starting transaction for {}'.format(table_name))
            response = rds_client.execute_statement(secretArn=db_secret_arn,database=db_name,resourceArn=db_arn,
                                                    sql="SET FOREIGN_KEY_CHECKS = 0;")

            response = rds_client.execute_statement(
                secretArn=db_secret_arn,
                database=db_name,
                resourceArn=db_arn,
                sql="DROP TABLE IF EXISTS {TABLE_NAME};".format(TABLE_NAME=table_name)
            )

            response = rds_client.execute_statement(secretArn=db_secret_arn, database=db_name, resourceArn=db_arn,
                                                    sql="SET FOREIGN_KEY_CHECKS = 1;")

            file_path = files_path + '/' + item
            file = open(file_path, 'r')
            file_lines = file.readlines()

            sql_statement = ''
            for line in file_lines:
                line = line.replace('\n', '')

                # escape blank lines
                if not line:
                    continue

                sql_statement += line
                # get full sql statement
                if line[-1] != ';':
                    continue

                # ignore semicolons in the statement
                sql_statement = sql_statement.replace(';', '')
                sql_statement += ';'

                response = rds_client.execute_statement(
                    secretArn=db_secret_arn,
                    database=db_name,
                    resourceArn=db_arn,
                    sql=sql_statement
                )
                sql_statement = ''

            print('Finished transaction for {}'.format(table_name))

    os.remove(download_path)
    print('Completed.')

