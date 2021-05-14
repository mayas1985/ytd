import uuid
import time
from common import *
from datetime import date
import watchtower, logging
import os
from time import perf_counter

logger.addHandler(watchtower.CloudWatchLogHandler(log_group='videotrimmer', stream_name=str(date.today()), use_queues=False,))


queue_url = trim_queue_url
bucket_name = 'mealcast-video-ouput-dev'

def runBash(command):
	os.system(command)

def crop(start,end,input,output):
	str = "ffmpeg -i " + input + " -ss  " + start + " -to " + end + " -c:v copy -c:a copy -y " + output
	logger.info(str)
	runBash(str)





while(True):
	msg = read_sqs(queue_url)
	cleanup_files=[]
	try:
		if msg is not None:
			t1_start = perf_counter() 
			logger.info(msg)
			jsn = json.loads(msg['Body'])
			input_url = jsn['input_url']
			output_url = jsn['output_url']
			starttime = jsn['starttime'] 
			endtime = jsn['endtime']
			input_bucket, input_key, input_filename = get_bucket_and_key_filepart(input_url)
			output_bucket, output_key, output_filename = get_bucket_and_key_filepart(output_url)
			
			with open(input_filename, 'wb') as f:
				s3.download_fileobj(input_bucket, input_key, f)
			logger.info('s3 downloaded video')
			cleanup_files.append(input_filename)
			crop(starttime,endtime,input_filename, output_filename)
			logger.info('Cropping successfully done')
			cleanup_files.append(output_filename)
			with open(output_filename, "rb") as f:
				s3.upload_fileobj(f, output_bucket, output_key)

			do_trim_callback(msg, True)

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
			do_trim_callback(msg, False)
	finally:
		for f in cleanup_files:
			delete_file(f)
			
	time.sleep(2)




