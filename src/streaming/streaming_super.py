import threading, logging, time
import random
import json
import MySQLdb
import redis
import time
import datetime
from kafka import KafkaConsumer
from kafka import KafkaProducer

# This script is a Kafka consumer for topic "transactions". It checks connection degree and send new messages with
# new topics "True" or "False" to server as a Kafka Producer for next data writings

class Streaming(threading.Thread):
    daemon = True

    
    def __init__(self):
        super(Streaming, self).__init__()
	
	self.db = MySQLdb.connect(host="ec2-yy-xxx-yy-xxx.compute-1.amazonaws.com", user="user", passwd="password", db="VenmoDB")
	self.rediska = redis.StrictRedis(host='ec2-xx-xxx-xx-xxx.compute-1.amazonaws.com', port=6379, db=0)
	self.rediska_from = redis.StrictRedis(host='ec2-xx-xxx-xx-xxx.compute-1.amazonaws.com', port=6379, db=1)
	self.rediska_to = redis.StrictRedis(host='ec2-xx-xxx-xx-xxx.compute-1.amazonaws.com', port=6379, db=2)
    
    def __filter_nones__(self,transaction_data):
    	if transaction_data is not None:
        	return True
    	return False
        
    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='xx.xxx.xxx.xxx:9092',group_id="gruppa")
        consumer.subscribe(['transactions'])
	producer = KafkaProducer(bootstrap_servers='xx.xxx.xxx.xxx:9092')
        for message in consumer:
            msg = str(message.value)
            unwraped = self.__extract_data__(msg)
	    if self.__filter_nones__(unwraped):
            	result = self.__transaction_process__(unwraped,self.db, self.rediska, self.rediska_from, self.rediska_to)
		producer.send(str(result), message.value)
    
    # This function returns True if found zero, first or second degree connection 	
    def __check_friendship__(self,user1,user2,db):

        cur = db.cursor()
        if (cur.execute("Select * FROM Friends WHERE (ID1,ID2)=(%s, %s)", (user1,user2))>0):
                cur.close()
                return True
	else:
		friend1 = cur.execute("Select ID2 FROM Friends Where (ID1)=(%s)",(user1))
                if friend1<1:
                        return False
		friend1 = cur.fetchall()
                friend2 = cur.execute("Select ID2 FROM Friends Where (ID1)=(%s)",(user2))
                if friend2<1:
                        return False                
		friend2 = cur.fetchall()	
		if (len (set(friend1).intersection(friend2))>0):
			cur.close()
			gc.collect()
			return True
		else:
			row = [item[0] for item in friend1] + [0]
			friend11 = cur.execute("Select ID2 FROM Friends Where ID1 in %s",[row])
			friend11 = cur.fetchall()
			if (len(set(friend11).intersection(friend2))>0):
				cur.close()
				return True
			else:
	                        row = [item[0] for item in friend2] + [0]
        	                friend21 = cur.execute("Select ID2 FROM Friends Where ID1 in %s",[row])
                	        friend21 = cur.fetchall()
                       		if (len(set(friend21).intersection(friend1))>0):
                               		cur.close()
                                	return True

        gc.collect()
        return False
	
    # Transform string with transaction info(JSON) to dictionary
    def __extract_data__(self, json_obj):
        json_body = json.loads(json_obj)
    	try:
        	from_id = json_body['actor']['id']
       		is_business = json_body['actor']['is_business']

        	to_id = json_body['transactions'][0]['target']['id']
        	is_business = is_business or json_body['transactions'][0]['target']['is_business']

        	if is_business is True:
            		return None
    	except:
        	return None

	data = {'from_id': int(from_id),
            	'to_id': int(to_id)}
    	return data

    def __transaction_process__(self,transaction_data, db, rediska, rediska1, rediska2):

    
    	user1 = transaction_data['from_id']
    	user2 = transaction_data['to_id']
    	
    	result = self.__check_friendship__(user1,user2,db)

	ts = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')	
	rediska.lpush(ts, result)
	if not result:
		ts = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M')
		rediska1.lpush(ts,user1)
		rediska2.lpush(ts,user2)
    	gc.collect()
    	return result



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


