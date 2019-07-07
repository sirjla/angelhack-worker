import json
import urllib
import os
import os.path
import sys
import requests

envLambdaTaskRoot = os.environ["LAMBDA_TASK_ROOT"]
sys.path.insert(0,envLambdaTaskRoot+"/parse_doc")

import boto3
import time


def startJob(s3BucketName, objectName):
    response = None
    client = boto3.client('textract')
    response = client.start_document_text_detection(
    DocumentLocation={
        'S3Object': {
            'Bucket': s3BucketName,
            'Name': objectName
        }
    })

    return response["JobId"]

def isJobComplete(jobId):
    time.sleep(5)
    client = boto3.client('textract')
    response = client.get_document_text_detection(JobId=jobId)
    status = response["JobStatus"]
    print("Job status: {}".format(status))

    while(status == "IN_PROGRESS"):
        time.sleep(5)
        response = client.get_document_text_detection(JobId=jobId)
        status = response["JobStatus"]
        print("Job status: {}".format(status))

    return status

def getJobResults(jobId):

    pages = []

    time.sleep(5)

    client = boto3.client('textract')
    response = client.get_document_text_detection(JobId=jobId)
    
    pages.append(response)
    print("Resultset page recieved: {}".format(len(pages)))
    nextToken = None
    if('NextToken' in response):
        nextToken = response['NextToken']

    while(nextToken):
        time.sleep(5)

        response = client.get_document_text_detection(JobId=jobId, NextToken=nextToken)

        pages.append(response)
        print("Resultset page recieved: {}".format(len(pages)))
        nextToken = None
        if('NextToken' in response):
            nextToken = response['NextToken']

    return pages


def lambda_handler(event, context):
    client = boto3.client('comprehend')
    dynamodb = boto3.resource('dynamodb')
    s3 = boto3.client('s3')
    print("WIIIIWUUUUU")
    print(event)
    bucket = "textract-console-us-east-1-ad4c7019-958d-4c8f-b18f-5ff0d167a4d1"
    metadata = event['Item']
    if metadata['pdf'] is not None:
        pdf = requests.get(metadata['pdf'], stream=True)
        if pdf.status_code == 200:
            with open('/tmp/{}.pdf'.format(metadata['title']), 'wb') as f:
                f.write(pdf.content)
                response = s3.upload_file(
                    '/tmp/{}.pdf'.format(metadata['title']),
                    'textract-console-us-east-1-ad4c7019-958d-4c8f-b18f-5ff0d167a4d1',
                    '{}.pdf'.format(metadata['title'].replace(' ', '-'))
                )
        file_name = metadata['id']
        jobId = startJob(bucket, file_name)
        print("Started job with id: {}".format(jobId))
        if(isJobComplete(jobId)):
            response = getJobResults(jobId)

        text = []
        try:
            for resultPage in response:
                for item in resultPage["Blocks"]:
                    if item["BlockType"] == "LINE":
                        text.append(item["Text"])
        except:
            return 200

        message = ' '.join(text).split(' ')
        
        lst = []
        max_s = 5000
        s = 0
        curr = -1
        for i in range(len(message)):
            if curr + len(message[i]) >= max_s:
                lst.append(' '.join(message[s:i]))
                s = i
                curr = -1
            curr += len(message[i]) + 1
        
        r_e = client.batch_detect_entities(
            TextList=lst,
            LanguageCode='en'
        )
        entities = list(map(lambda x: x['Text'], filter(lambda e: e['Score'] > 0.9, r_e['ResultList'][0]['Entities'])))
        
        r_k = client.batch_detect_key_phrases(
            TextList=lst,
            LanguageCode='en'
        )
        keys = list(map(lambda x: x['Text'], filter(lambda e: e['Score'] > 0.9, r_k['ResultList'][0]['KeyPhrases'])))
        metadata['keywords'] = '; '.join(keys)
        metadata['entities'] = '; '.join(entities)
        table = dynamodb.Table('documents-ahfront')
        table.put_item(
            Item=metadata
        )
        return 200

    return 200
