
import botocore
import boto3
import threading, logging, time
from kafka import KafkaProducer
import smart_open

class Producer(threading.Thread):
    daemon = True

    def run(self):
        producer = KafkaProducer(bootstrap_servers='34.226.228.0:9092')

        bucket_name = 'venmo-json'
        bucket = self.__get_s3_bucket__(bucket_name)
        # Send data from S3 to Kafka queue
	for i in range(1,28):
		for line in  smart_open.smart_open("s3://venmo-json/2017_01/venmo_2017_01_"+str(i).zfill(2)+".json"):
        #for obj in bucket.objects.filter(Prefix='2017_*'):
	   # print("m")
           # data = obj.get()['Body']  
           # json_body = data.read().splitlines()
           # for json_obj in json_body:
                	producer.send('transactions', line)
                	#time.sleep(0.05)
                	#print type(line)# + '\n' + '=================================================================' + '\n'

    # Access S3 bucket
    def __get_s3_bucket__(self, bucket_name):
        s3 = boto3.resource('s3')
        try:
            s3.meta.client.head_bucket(Bucket=bucket_name)
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                exists = False
                print (e.response['404 Error: bucket not found'])
            else:
                print (e.response['Error'])
            return None
        else:
            return s3.Bucket(bucket_name)


if __name__ == "__main__":

    producer = Producer()
    producer.start()
    while True:
        time.sleep(10)
