const stream = require('stream');
const youtubedl = require('youtube-dl');
const AWS = require('aws-sdk');
const regionp = 'us-west-1';
AWS.config.update({region: regionp });
const logger = require('winston'),
winstonCloudWatch = require('winston-cloudwatch');

var self = logger.add(new winstonCloudWatch({
  name: 'awslogger',
  logGroupName: 'YoutubeDownloader',
  logStreamName: new Date().toDateString(),
  awsRegion: regionp
}));
	
// Create an SQS service object
var sqs = new AWS.SQS({apiVersion: '2012-11-05'});
const queueURL = "https://sqs.us-west-1.amazonaws.com/749678555276/temp-sqs";

exports.handler = function(url, message) {   
  
  const s3 = new AWS.S3({apiVersion: '2006-03-01'});
  const passtrough = new stream.PassThrough();
  const dl = youtubedl(url, ['--format=bestvidoe+bestaudio'], {maxBuffer: Infinity});
  dl.once('error', (err) => {
    logger.error(JSON.stringify(err));
  });
  key = 'test.mkv';//message.MessageId + '.mp4';
  const upload = new AWS.S3.ManagedUpload({
    params: {
      Bucket: 'deletelaterfortesting',
      Key: key,
      Body: passtrough
    },
    partSize: 1024 * 1024 * 64 // 64 MB in bytes part upload
  });
  upload.on('httpUploadProgress', (progress) => {
    logger.info(` copying video ...`, progress);
  });
  upload.send((err) => {
	   if (err) {
		logger.error(`error while upload send ${JSON.stringify(err)}` );
	  } else {
		  logger.info('video uploaded to s3');
		deleteSqsMessage(message);
	  }
  });
  dl.pipe(passtrough);
}


exports.readSqs = function(onRecieveMessage){
	// Set the region
	var params = {
	 MaxNumberOfMessages: 10,
	 MessageAttributeNames: [
		"All"
	 ],
	 QueueUrl: queueURL,
	 VisibilityTimeout: 20,
	 WaitTimeSeconds: 0
	};

	sqs.receiveMessage(params, function(err, data) {
		
	  if (err) {
		logger.error("Receive Error", err);
	  } else if (data.Messages) {
		onRecieveMessage(data.Messages);
	  }
	});
}

function onRecieveMessage(messages){
	
	logger.info('onReceiveResponse ' + JSON.stringify(messages));
	messages.forEach(message => {
		logger.info('current processing msg ' +  JSON.stringify(messages))
		obj = JSON.parse(message.Body);
		logger.info('current url ' + obj.url);
		exports.handler(obj.url, message);
	});
}

function deleteSqsMessage(message){
	 var deleteParams = {
						 QueueUrl: queueURL,
						 ReceiptHandle: message.ReceiptHandle
						 };
	 sqs.deleteMessage(deleteParams, function(err, data) {
	 if (err) {
			logger.error("Delete Error", err);
		 } else {
			logger.info("Message Deleted fom sqs " + message.MessageId);
		 }
	 });
}

process.on('uncaughtException', function(err) {
	console.log(err);
	logger.log('error', 'Fatal uncaught exception crashed cluster', err, function(err, level, msg, meta) {
        process.exit(1);
    });
});


//exports.readSqs(onRecieveMessage);
exports.handler('https://www.youtube.com/watch?v=Pkh8UtuejGw', null);

// console.log('flushing')
// // flushes the logs and clears setInterval
// var transport = self.transports.find((t) => t.name === 'awslogger')
// console.log(transport)
// transport.kthxbye(function() {
  // console.log('exiting');
// });