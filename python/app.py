import time
import json
import boto3
import youtube_dl
import watchtower, logging
from datetime import date

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.addHandler(watchtower.CloudWatchLogHandler(log_group='youtubedownloader', stream_name=str(date.today()), use_queues=False,))

# Create SQS client
sqs = boto3.client('sqs')
s3 = boto3.client('s3')
queue_url = 'https://sqs.us-west-1.amazonaws.com/749678555276/ytd-request-queue'
bucket_name = 'mealcast-video-ouput-dev'
print(queue_url)

def read_sqs(queue_url):
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
		

def download_video(url, filename):
	url = 'https://www.youtube.com/watch?v=LXb3EKWsInQ'
	ydl_opts = {
		#"format": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/bestvideo+bestaudio",
		"format": "best[ext=mp4]",
		"outtmpl": filename,
		"merge-output-format": "mp4"
	}
	with youtube_dl.YoutubeDL(ydl_opts) as ydl:
		ydl.download([url])


def get_url(message):
	jsn = json.loads(message['Body'])
	url = jsn['url']
	return url
	# receipt_handle = message['ReceiptHandle']
	
def get_video_filename(message):
	return message['MessageId'] + ".mp4"
	



def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def validate_file(file_name):
	pass

def delete_message(receipt_handle):
	response = sqs.delete_message(
	QueueUrl=queue_url,
	ReceiptHandle=receipt_handle)
	logger.info(response)

while(True):
	msg = read_sqs(queue_url)
	if msg is not None:
		logger.info(msg)
		url = get_url(msg)
		download_video(url, get_video_filename(msg))
		logger.info('downloaded video')
		validate_file(get_video_filename(msg))

		with open(get_video_filename(msg), "rb") as f:
			s3.upload_fileobj(f, bucket_name, get_video_filename(msg))
		logger.info('s3 upload video')
		delete_message(msg['ReceiptHandle'])
	else:
		logger.info('no msg in queue')
	time.sleep(30)

