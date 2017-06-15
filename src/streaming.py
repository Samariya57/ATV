import threading, logging, time
import random
import json
import MySQLdb
from kafka import KafkaConsumer
import gc



class Streaming(threading.Thread):
    daemon = True

    
    def __init__(self):
        super(Streaming, self).__init__()
	
	self.db = MySQLdb.connect(host="ec2-54-158-19-194.compute-1.amazonaws.com", user="venmo", passwd="pass", db="VenmoDB")

    def __filter_nones__(self,transaction_data):
    	if transaction_data is not None:
        	return True
    	return False
        
    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='34.226.228.0:9092',group_id="gruppa")
        consumer.subscribe(['transactions'])

        for message in consumer:
            msg = str(message.value)
            unwraped = self.__extract_data__(msg)
	    if self.__filter_nones__(unwraped):
            	result = self.__transaction_between__(unwraped,self.db)
    def __check_friendship__(self,user1,user2,db):

        cur = db.cursor()
        if (cur.execute("Select * FROM Friends WHERE (ID1,ID2)=(%s, %s)", (user1,user2))>0):
                cur.close()
                return True
        try:
                cur.execute("INSERT INTO Friends VALUE (%s,%s)", [(user1,user2),(user2,user1)])
                #cur.execute("INSERT INTO Friends VALUE (%s,%s)", (user2,user1))
                db.commit()
                cur.close()
        except:
                cur.close()
        gc.collect()
        return False

	
    def __rec_user__(self,ID1, FullName1, ID2, FullName2, db):
        cur = db.cursor()
        try:
                cur.execute("INSERT INTO Users VALUE (%s,%s)", (ID1,FullName1))
                db.commit()
                cur.execute("INSERT INTO Users VALUE (%s,%s)", (ID2,FullName2))
                db.commit()
	except:
		cur.close()
	cur.close()

    
    def __extract_data__(self, json_obj):
        json_body = json.loads(json_obj)
    	try:
        	from_id = json_body['actor']['id']
        #from_firstname = json_body['actor']['firstname']
        #from_lastname = json_body['actor']['lastname']
        	from_username = json_body['actor']['username']
        	from_picture = json_body['actor']['picture']
       		is_business = json_body['actor']['is_business']

        	to_id = json_body['transactions'][0]['target']['id']
        #to_firstname = json_body['transactions'][0]['target']['firstname']
        #to_lastname = json_body['transactions'][0]['target']['lastname']
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
        	#'from_firstname': from_firstname,
            	#'from_lastname': from_lastname,
            	'from_username': from_username,
            	'from_picture': from_picture,
            	'to_id': int(to_id),
            	#'to_firstname': to_firstname,
           	#'to_lastname': to_lastname,
            	'to_username': to_username,
            	'to_picture': to_picture,
           	'message': message,
            	'timestamp': timestamp,
            	'payment_id': int(payment_id),
            	'amount': random.randint(5,75)}
    	return data

    def __transaction_between__(self,transaction_data, db):

    
    	user1 = transaction_data['from_id']
    	user2 = transaction_data['to_id']
    	user1FN = transaction_data['from_username']
    	user2FN = transaction_data['to_username']

    	payment_id = transaction_data['payment_id']
    	amount = transaction_data['amount']
    	time_t = transaction_data['timestamp']
    	message = transaction_data['message']

    	result = self.__check_friendship__(user1,user2,db)
    	self.__rec_user__(user1,user1FN,user2,user2FN,db)

    	try:
        	cur=db.cursor()
        	cur.execute("INSERT INTO Transactions VALUE (%s,%s,%s,%s,%s,%s,%s,%s,%s)",(payment_id, user1,user1FN,user2,user2FN,time_t, message,amount,result))
        	db.commit()
        	cur.close()
        	#db.close()
    	except:
        	cur.close()
        	#db.close()
    	gc.collect()
    	return True



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


