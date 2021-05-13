import os
import requests
import json
import boto3

import watchtower, logging
# Create SQS client
sqs = boto3.client('sqs')
s3 = boto3.client('s3')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__file__)

def do_callback(msg, result):
	try:
		jsn = json.loads(msg['Body'])
		url = jsn['callback_url']
		payload = jsn['callback_payload']
		payload['trimStatus'] = result
		logger.info({"url": url, "payload": payload } )
		x = requests.put(url, data = payload)
		logger.info(x)
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