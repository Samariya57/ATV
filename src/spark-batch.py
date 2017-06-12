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
#from kafka import KafkaProducer
#from sets import Set
# import boto3
import random
#import time
#import datetime

def extract_data(json_body):

    #print("extract_data")
    json_body = json.loads(json_body)


    try:
        from_id = json_body['actor']['id']
        from_firstname = json_body['actor']['firstname']
        from_lastname = json_body['actor']['lastname']
        from_username = json_body['actor']['username']
        from_picture = json_body['actor']['picture']
        is_business = json_body['actor']['is_business']

        to_id = json_body['transactions'][0]['target']['id']
        to_firstname = json_body['transactions'][0]['target']['firstname']
        to_lastname = json_body['transactions'][0]['target']['lastname']
        to_username = json_body['transactions'][0]['target']['username']
        to_picture = json_body['transactions'][0]['target']['picture']
        is_business = is_business or json_body['transactions'][0]['target']['is_business']

	payment_id = json_body['payment_id']
	
        if is_business is True:
            return None

        # Transaction data
        message = json_body['message']
        timestamp = json_body['created_time']
    except:
        return None

    data = {'from_id': int(from_id),
            'from_firstname': from_firstname,
            'from_lastname': from_lastname,
            'from_username': from_username,
            'from_picture': from_picture,
            'to_id': int(to_id),
            'to_firstname': to_firstname,
            'to_lastname': to_lastname,
            'to_username': to_username,
            'to_picture': to_picture,
            'message': message,
            'timestamp': timestamp,
	    'payment_id': int(payment_id),
	    'amount': random.randint(5,75)}
    return data

def filter_nones(transaction_data):
    if transaction_data is not None:
        return True
    return False

def check_friendship(user1,user2, db):

        #db = MySQLdb.connect(host="ec2-54-158-19-194.compute-1.amazonaws.com", user="venmo", passwd="pass", db="VenmoDB")
        cur = db.cursor()
        if (cur.execute("Select * FROM Friends_NT WHERE (ID1,ID2)=(%s, %s)", (user1,user2))>0):
                cur.close()
                #db.close()
                return True
	try:
        	cur.execute("INSERT INTO Friends_NT VALUE (%s,%s)", (user1,user2))
		db.commit()
		cur.close()
        except:
		cur.close()
        #db.close()
	gc.collect()
        return False




def transaction_between(transaction_data, db):

    #print("transaction_between")
    user1 = transaction_data['from_id']
    user2 = transaction_data['to_id']
    payment_id = transaction_data['payment_id']	
    amount = transaction_data['amount']
    result = check_friendship(user1,user2, db)
    try:
	cur=db.cursor()
	cur.execute("INSERT INTO Transaction_NT VALUE (%s,%s,%s,%s,%s)",(payment_id, user1,user2,amount,result))
	cur.execute("INSERT INTO Transaction_NT VALUE (%s,%s,%s,%s,%s)",(-payment_id, user2,user1,-amount,result))
	db.commit()	
	cur.close()
	db.close()
    except:
	cur.close()
	db.close()
    gc.collect()
    return True    


# To submit script:
# $SPARK_HOME/bin/spark-submit --master spark://34.225.200.18:7077 --executor-memory 6G spark_batch.py
if __name__ == "__main__":

    #db = MySQLdb.connect(host="ec2-54-158-19-194.compute-1.amazonaws.com", user="venmo", passwd="pass", db="VenmoDB")
    #cur = db.cursor()
    #print( (cur.execute("Select * FROM Friends_NT WHERE (ID1,ID2)=(%s, %s)", (123,563))))
    #cur.close()        

    list_rez=[]
    sc = SparkContext(appName="Venmo")
    for i in range(1,13):

        read_rdd = sc.textFile("s3a://venmo-json/2015_"+str(i).zfill(2)+"/*")

        cleaned_rdd = read_rdd.map(lambda x: extract_data(x)).filter(lambda x: filter_nones(x)) # clean json data
    #print(cleaned_rdd.collect())
        check_friends = cleaned_rdd.map(lambda x: transaction_between(x,MySQLdb.connect(host="ec2-54-158-19-194.compute-1.amazonaws.com", user="venmo", passwd="pass", db="VenmoDB")))
    #check_friends.saveAsTextFile("s3a://venmo-json/2012_04.out"+v)
    #print(check_friends.count())
    #print(check_friends.countByValue())
        list_rez.append(check_friends.count())
	gc.collect()
    print(sum(list_rez))

