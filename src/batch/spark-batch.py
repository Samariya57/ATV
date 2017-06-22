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

# JSON -> dictionary
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
	    'amount': random.randint(5,75)}
    return data

# Filter nones (transaction with at least one business person)
def filter_nones(transaction_data):
    if transaction_data is not None:
        return True
    return False

# Check zero degree connection between two users and write them as friends
def check_friendship(user1,user2,db):

        cur = db.cursor()
        if (cur.execute("Select * FROM Friends WHERE (ID1,ID2)=(%s, %s)", (user1,user2))>0):
                cur.close()
                return True
	try:
        	cur.execute("INSERT INTO Friends VALUE (%s,%s)", (user1,user2))
		cur.execute("INSERT INTO Friends VALUE (%s,%s)", (user2,user1))
		db.commit()
		cur.close()
        except:
		cur.close()
	gc.collect()
        return False
# Write new user info into DB (MySQL)
def rec_user(ID1, FullName1, ID2, FullName2, db):
	cur = db.cursor()
	if (cur.execute("Select * FROM Users WHERE ID=%s", (ID1))<1):
		cur.execute("INSERT INTO Users VALUE (%s,%s)", (ID1,FullName1))
		db.commit()
        if (cur.execute("Select * FROM Users WHERE ID=%s", (ID2))<1):
                cur.execute("INSERT INTO Users VALUE (%s,%s)", (ID2,FullName2))
                db.commit()
	cur.close()

# Transaction processing: check users, check friendship, write transaction into DB
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
	db.commit()	
    except:
	db.rollback()
    db.close()
    return True    


# To submit script:
# $SPARK_HOME/bin/spark-submit --master spark://34.225.200.18:7077 --executor-memory 6G spark_batch.py
if __name__ == "__main__":

    list_rez=[]
    sc = SparkContext(appName="Venmo")
    for i in range(1,13):

        read_rdd = sc.textFile("s3a://venmo-json/2015_"+str(i).zfill(2)+"/*")

        cleaned_rdd = read_rdd.map(lambda x: extract_data(x)).filter(lambda x: filter_nones(x)) # clean json data
        check_friends = cleaned_rdd.map(lambda x: transaction_between(x,MySQLdb.connect(host="ec2-xx-xx-xx-xxx.compute-1.amazonaws.com", user="user", passwd="password", db="VenmoDB")))

        list_rez.append(check_friends.count())
	gc.collect()
    print(sum(list_rez))

