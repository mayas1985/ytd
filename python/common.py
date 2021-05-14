import os
import requests
import json
import boto3

import watchtower, logging


# Create SQS client
sqs = boto3.client('sqs')
s3 = boto3.client('s3')
sns = boto3.client('sns', region_name='us-east-1')
trim_sns_arn = 'arn:aws:sns:us-east-1:749678555276:dev_trimcontent_received'
download_queue_url = 'https://sqs.us-west-1.amazonaws.com/749678555276/ytd-request-queue'
trim_queue_url = 'https://sqs.us-west-1.amazonaws.com/749678555276/vtrim-request-queue'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__file__)

def do_ytd_callback(msg, result):
	try:
		logger.info("callback not implemented yet")
	except Exception as e: 
		print(e)
		logger.error(e)

def do_trim_callback(msg, result):
	try:
		jsn = json.loads(msg['Body'])
		#sns = jsn['callback_sns']
		info = jsn['callback_payload']
		info['status'] = result
		request_body = {
			"payload": json.dumps(info),
			 "name": "TrimStatusReceived",
			 "reconciliationId": 0
		}
		
		logger.info({"sns": sns, "payload": request_body } )
		response = sns.publish(
			TopicArn=trim_sns_arn,
			Message=json.dumps(request_body)
			
		)
		logger.info(response)
	except Exception as e: 
		print(e)
		logger.error(e)



def delete_file(fname):
	if os.path.exists(fname):
		os.remove(fname)
	else:
		print("The file does not exist")


def get_bucket_and_key_filepart(s3_url):
	bucket, key = s3_url.replace("s3://", "").split("/", 1)
	filename = s3_url.replace("s3://", "").split("/")[-1]
	return bucket, key, filename

def change_message_visibility(queue_url, handle):
	sqs.change_message_visibility(
	QueueUrl=queue_url,
	ReceiptHandle=handle,
	VisibilityTimeout=1
	)

def read_sqs(queue_url):
	try:
		# Receive message from SQS queue
		response = sqs.receive_message(
		QueueUrl=queue_url,
		# AttributeNames=[
		# 'SentTimestamp'
		# ],
		MaxNumberOfMessages=1,
		# MessageAttributeNames=[
		# 'All'
		# ],
		VisibilityTimeout=4000,
		# WaitTimeSeconds=0
		)
		
		if response.get('Messages') is not None:
			return response['Messages'][0]
		else:	
			return None
	except Exception as e: 
		print(e)
		logger.error(e)

def get_url(message):
	jsn = json.loads(message['Body'])
	url = jsn['url']
	return url
	# receipt_handle = message['ReceiptHandle']


def delete_message(queue_url, receipt_handle):
	response = sqs.delete_message(
	QueueUrl=queue_url,
	ReceiptHandle=receipt_handle)
#do_trim_callback({'Body': '{\"input_url\":\"s3://dev-orchestration/Videos/Base/609e35d593354d2fffb47dd7/65a33e52-7fbc-4510-a819-1a6bea601060.mp4\",\"output_url\":\"s3://dev-orchestration/Videos/Preview/609e35d593354d2fffb47dd7/9fffa815-6ce2-46d8-9a34-df5debfca15c.mp4\",\"starttime\":\"00:00:00\",\"endtime\":\"00:00:14\",\"callback_sns\":null,\"callback_payload\":{\"recipeDetailWorkflowId\":\"609e35d593354d2fffb47dd7\"}}'}, True)