import requests
import boto3
#magic url to get instance data
#requests.get('http://ip-172-31-2-9.us-west-1.compute.internal/latest/meta-data/instance-id')
#instance_id = response.text
instance_id='i-0210d108ffa8eb101'
client = boto3.client('ec2')
response = client.describe_tags( 
		Filters=[
        {
            'Name': 'resource-id',
            'Values': [
                instance_id,
            ]
        },
    ],

)
for tag in response['Tags']:
    if tag['Key']=='trim_sns_arn':
        trim_sns_arn = tag['Value']
    elif tag['Key']=='download_queue_url':
        download_queue_url = tag['Value']
    elif tag['Key']=='trim_queue_url':
        trim_queue_url = tag['Value']

print(trim_sns_arn, download_queue_url, trim_queue_url)
if (trim_sns_arn == None or download_queue_url == None or trim_queue_url == None):
    raise Exception("ec2 meta tags not set properly")


