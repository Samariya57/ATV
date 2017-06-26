# This script write transaction to DB after getting a flag False

from kafka import KafkaConsumer
from kafka import KafkaProducer
import threading, logging, time
import random
import json
import MySQLdb
import gc
import time
import datetime


class Streaming(threading.Thread):
    daemon = True
    
    def __init__(self):
        super(Streaming, self).__init__()
	self.db = MySQLdb.connect(host="ec2-.compute-1.amazonaws.com", user="", passwd="", db="VenmoDB")

    def __filter_nones__(self,transaction_data):
    	if transaction_data is not None:
            return True
    	return False
        
    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='host:9092')
        consumer.subscribe(['False'])
	producer = KafkaProducer(bootstrap_servers='host:9092')
        for message in consumer:
            msg = str(message.value)
            unwraped = self.__extract_data__(msg)
	    if self.__filter_nones__(unwraped):
            	result = self.__write_transaction__(unwraped,self.db)
        return False
	
    
    def __extract_data__(self, json_obj):
        json_body = json.loads(json_obj)
    	try:
            from_id = json_body['actor']['id']
            from_username = json_body['actor']['username']
            from_picture = json_body['actor']['picture']
       	    is_business = json_body['actor']['is_business']

            to_id = json_body['transactions'][0]['target']['id']
            to_username = json_body['transactions'][0]['target']['username']
            to_picture = json_body['transactions'][0]['target']['picture']
            is_business = is_business or json_body['transactions'][0]['target']['is_business']

            if is_business is True:
            	return None

            # Transaction data
            payment_id = json_body['payment_id']
            message = json_body['message']
            timestamp = json_body['created_time']
    	except:
            return None

	data = {'from_id': int(from_id),
            	'from_username': from_username,
            	'from_picture': from_picture,
            	'to_id': int(to_id),
            	'to_username': to_username,
            	'to_picture': to_picture,
           	'message': message,
            	'timestamp': timestamp,
                'payment_id': int(payment_id),
            	'amount': random.randint(5,75)}
    	return data

    def __write_transaction__(self,transaction_data, db):
    	user1 = transaction_data['from_id']
    	user2 = transaction_data['to_id']
        user1FN = transaction_data['from_username']
        user2FN = transaction_data['to_username']
   
        payment_id = transaction_data['payment_id']	
        amount = transaction_data['amount']
        time_t = transaction_data['timestamp']
        message = transaction_data['message']
    
        result = False
        try:
	    cur=db.cursor()
	    cur.execute("INSERT INTO Transactions VALUE (%s,%s,%s,%s,%s,%s,%s,%s,%s)",(payment_id, user1,user1FN,user2,user2FN,time_t, message,amount,result))
	    db.commit()
	    cur.close()
	    return True	
    	except:
	    db.rollback()
	    cur.close()    
	    return False 


if __name__ == "__main__":

    thread = Streaming()

    while True:
        if not thread.isAlive():
            print("Starting Kafka consumer...")
            thread.start()
            print("Started Kafka consumer.")
        else:
            print("Listening for new messages in topic: 'transactions'...")
            time.sleep(10)


