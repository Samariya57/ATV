from __future__ import print_function

import sys

# To import next library you have to install some packages
# sudo apt-get update
# sudo apt-get install python-mysqldb
# sudo apt-get install mysql-client-core-5.5

import MySQLdb
import json
import gc
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from botocore.exceptions import ClientError
import random
from itertools import repeat
def extract_data(json_body):

    
    json_body = json.loads(json_body)


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
	    'amount': random.randint(5,75),
            'AV': False}
    return data

def filter_nones(transaction_data):
    if transaction_data is not None:
        return True
    return False

def check_friends (transaction_data,db):
	#db=MySQLdb.connect(host="ec2-54-158-19-194.compute-1.amazonaws.com", user="venmo", passwd="pass", db="VenmoDB")
	user1 = transaction_data['from_id']
	user2 = transaction_data['to_id']
        cur = db.cursor()
        if (cur.execute("Select * FROM Friends WHERE (ID1,ID2)=(%s, %s)", (user1,user2))>0):
                #cur.fetchall()
                #db.close()
		#gc.collect()
                transaction_data['AV']=True
	else:
		try:
        		cur.execute("INSERT INTO Friends VALUE (%s,%s)", (user1,user2))
			db.commit()
			cur.execute("INSERT INTO Friends VALUE (%s,%s)", (user2,user1))
			#cur.fetchall()
			db.commit()
			#cur.close()
        	except:
			db.rollback()
	cur.close()
	#db.close()        
	#gc.collect()
        return transaction_data

def rec_user(ID1, FullName1, ID2, FullName2,db):
	#db=MySQLdb.connect(host="ec2-54-158-19-194.compute-1.amazonaws.com", user="venmo", passwd="pass", db="VenmoDB")
	cur = db.cursor()
	i=0
	if (cur.execute("Select * FROM Users WHERE ID=%s", (ID1))<1):
		#cur.fetchall()
		cur.execute("INSERT INTO Users VALUE (%s,%s)", (ID1,FullName1))
		i = i+1
		db.commit()
        if (cur.execute("Select * FROM Users WHERE ID=%s", (ID2))<1):
		#cur.fetchall()
                cur.execute("INSERT INTO Users VALUE (%s,%s)", (ID2,FullName2))
		i = i+1
                db.commit()
	cur.close()
	#db.close()
	#gc.collect()
	if i<1:
		return True; return False
	

def transaction_between(transaction_data, db):

    
    user1 = transaction_data['from_id']
    user2 = transaction_data['to_id']
    user1FN = transaction_data['from_username']
    user2FN = transaction_data['to_username']
   
    payment_id = transaction_data['payment_id']	
    amount = transaction_data['amount']
    time_t = transaction_data['timestamp']
    message = transaction_data['message']
    
    result = check_friendship(user1,user2,db)
    rec_user(user1,user1FN,user2,user2FN,db)
    
    try:
	cur=db.cursor()
	cur.execute("INSERT INTO Transactions VALUE (%s,%s,%s,%s,%s,%s,%s,%s,%s)",(payment_id, user1,user1FN,user2,user2FN,time_t, message,amount,result))
	#cur.execute("INSERT INTO Transaction_NT VALUE (%s,%s,%s,%s,%s)",(-payment_id, user2,user1,-amount,result))
	db.commit()	
	cur.close()
	db.close()
    except:
	cur.close()
	db.close()
    gc.collect()
    return True    

def processing (partition):
	#db1=MySQLdb.connect(host="ec2-54-158-19-194.compute-1.amazonaws.com", user="venmo", passwd="pass", db="VenmoDB")
        db=MySQLdb.connect(host="ec2-54-158-19-194.compute-1.amazonaws.com", user="venmo", passwd="pass", db="VenmoDB")	
	for x in partition:
		if rec_user(x['from_id'],x['from_username'],x['to_id'],x['to_username'],db):
			check_friends(x,db)
	db.close()
	return partition

# To submit script:
# $SPARK_HOME/bin/spark-submit --master spark://34.225.200.18:7077 --executor-memory 6G spark_batch.py
if __name__ == "__main__":

    #list_rez=[]
    sc = SparkContext(appName="Venmo")
    
    #db2=MySQLdb.connect(host="ec2-54-158-19-194.compute-1.amazonaws.com", user="venmo", passwd="pass", db="VenmoDB")
    for i in range(12,13):

        #read_rdd = sc.textFile("s3a://venmo-json/2016_"+str(i).zfill(2)+"/*")
	read_rdd = sc.textFile("s3a://venmo-json/2016_12/venmo_2016_12_"+str(i).zfill(2)+".json")
	cleaned_rdd = read_rdd.map(lambda x: extract_data(x)).filter(lambda x: filter_nones(x)) # clean json data
    
	print(cleaned_rdd.count())
        transactions_to_write = cleaned_rdd.mapPartitions(processing)
        print(transactions_to_write.count())
	print(cleaned_rdd.count())
	#gc.collect()
    #db1.close()
    #db2.close()
    #print(sum(list_rez))
