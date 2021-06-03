from common import *
import time
import json
import boto3
import youtube_dl
import watchtower, logging
from datetime import date
from time import perf_counter
from env import *

logger.addHandler(watchtower.CloudWatchLogHandler(log_group='youtubedownloader', stream_name=str(date.today()), use_queues=False,))


queue_url = download_queue_url
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

def validate_file(file_name):
	pass

while(True):
	msg = read_sqs(queue_url)
	cleanup_files=[]
	try:
		if msg is not None:
			t1_start = perf_counter() 
			logger.info(msg)
			jsn = json.loads(msg['Body'])
			yt_url = jsn['yt_url']
			output_url = jsn['output_url']
			output_bucket, output_key, output_filename = get_bucket_and_key_filepart(output_url)
			download_video(yt_url, output_filename)
			logger.info('downloaded video')
			validate_file(output_filename)
			cleanup_files.append(output_filename)
			
			with open(output_filename, "rb") as f:
				s3.upload_fileobj(f, output_bucket, output_filename)
			logger.info('s3 upload video')
			delete_message(queue_url, msg['ReceiptHandle'])
			t1_stop = perf_counter()
			logger.info("Elapsed time during 1 iteration in seconds :" + str(t1_stop-t1_start))
		else:
			print('no msg in queue')
	except Exception as e: 
		print(e)
		logger.error(e)
		if msg is not None:
			change_message_visibility(queue_url, msg['ReceiptHandle'])
			logger.info('release sqs message')

	finally:
		for f in cleanup_files:
			delete_file(f)

	time.sleep(2)
	

