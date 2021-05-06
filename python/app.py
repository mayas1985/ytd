import json
import boto3
import youtube_dl

# Create SQS client
sqs = boto3.client('sqs')

# queue_url = 'https://sqs.us-west-1.amazonaws.com/749678555276/temp-sqs'
# print(queue_url)

# # Receive message from SQS queue
# response = sqs.receive_message(
    # QueueUrl=queue_url,
    # AttributeNames=[
        # 'SentTimestamp'
    # ],
    # MaxNumberOfMessages=1,
    # MessageAttributeNames=[
        # 'All'
    # ],
    # VisibilityTimeout=0,
    # WaitTimeSeconds=0
# )

# message = response['Messages'][0]
# receipt_handle = message['ReceiptHandle']


# jsn = json.loads(message['Body'])
#url = jsn['url']
filename = "test.mp4"
url = 'https://www.youtube.com/watch?v=Pkh8UtuejGw'
ydl_opts = {
    "format": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/bestvideo+bestaudio",
	"outtmpl": filename,
	"merge-output-format": "mp4"
}


with youtube_dl.YoutubeDL(ydl_opts) as ydl:
	ydl.download([url])

s3 = boto3.client('s3')
with open(filename, "rb") as f:
    s3.upload_fileobj(f, "deletelaterfortesting", filename)


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

