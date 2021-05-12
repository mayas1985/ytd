from common import *
import time
import json
import boto3
import youtube_dl
import watchtower, logging
from datetime import date

logger.addHandler(watchtower.CloudWatchLogHandler(log_group='youtubedownloader', stream_name=str(date.today()), use_queues=False,))


queue_url = 'https://sqs.us-west-1.amazonaws.com/749678555276/ytd-request-queue'
bucket_name = 'mealcast-video-ouput-dev'
print(queue_url)

def progress_hook(d):
	if d['status'] == 'finished':
		logger.info('progresshook - finished download')
	if d['status'] == 'downloading':
		logger.info(d['filename'] + " " + d['_percent_str']+" " +d['_eta_str'])

def download_video(url, filename):
	#url = 'https://www.youtube.com/watch?v=D_2DBLAt57c'
	ydl_opts = {
		#"format": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/bestvideo+bestaudio",
		"format": "bestvideo[height<=?1080][ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]",
		#"format": "best[ext=mp4]",
		'progress_hooks': [progress_hook],
		"outtmpl": filename,
		"merge-output-format": "mp4"
	}
	with youtube_dl.YoutubeDL(ydl_opts) as ydl:
		ydl.download([url])


	
def get_video_filename(message):
	return message['MessageId'] + ".mp4"
	


def validate_file(file_name):
	pass

while(True):
	msg = read_sqs(queue_url)
	try:
		if msg is not None:
			logger.info(msg)
			url = get_url(msg)
			download_video(url, get_video_filename(msg))
			logger.info('downloaded video')
			validate_file(get_video_filename(msg))

			with open(get_video_filename(msg), "rb") as f:
				s3.upload_fileobj(f, bucket_name, get_video_filename(msg))
			logger.info('s3 upload video')
			delete_message(queue_url, msg['ReceiptHandle'])
		else:
			print('no msg in queue')
	except Exception as e: 
		print(e)
		logger.error(e)
		if msg is not None:
			change_message_visibility(queue_url, msg['ReceiptHandle'])
			logger.info('release sqs message')
	time.sleep(2)
	

