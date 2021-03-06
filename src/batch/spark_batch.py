from __future__ import print_function

import sys

# To import next library you have to install some packages
# sudo apt-get update
# sudo apt-get install python-mysqldb
# sudo apt-get install mysql-client-core-5.5

import json
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
from botocore.exceptions import ClientError
import random
import mysql.connector as sqlcon


# JSON->dictionary
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
            'message': message.encode('ascii', 'ignore').decode('ascii')[0:250],
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
    user1 = transaction_data['from_id']
    user2 = transaction_data['to_id']
    cur = db.cursor()
    if (cur.execute("Select * FROM Friends WHERE (ID1,ID2)=(%s, %s)", (user1,user2))>0):
        transaction_data['AV']=True
    else:
	try:
            cur.executemany("INSERT INTO Friends VALUE (%s,%s)", ((user1,user2),(user2,user1)))
	    db.commit()
        except:
	    db.rollback()
	cur.close()
        return transaction_data

def rec_user(ID1, FullName1, ID2, FullName2,db):
    cursor = db.cursor(prepared = True)
    stmt_sel = "Select * FROM Users WHERE ID=(%s)"
    users = []
    if (cursor.execute(stmt_sel, (ID1,))<1):
	cursor.fetchall()
	users.append((ID1,FullName1))
    if (cursor.execute(stmt_sel, (ID2,))<1):
	cursor.fetchall()
	users.append((ID2,FullName2))
    cursor.close()
    return users
	

def write_transactions (partition):

    db=sqlcon.connect(host=".compute-1.amazonaws.com", user="", password="", database="VenmoDB")
    cur=db.cursor()
    transactions = []
    results = []
    for transaction_data in partition:
        user1 = transaction_data['from_id']
        user2 = transaction_data['to_id']
    	user1FN = transaction_data['from_username']
    	user2FN = transaction_data['to_username']
   
    	payment_id = transaction_data['payment_id']	
    	amount = transaction_data['amount']
    	time_t = transaction_data['timestamp']
    	message = transaction_data['message']
    
    	result = transaction_data['AV']
    	transactions.append((payment_id, user1,user1FN,user2,user2FN,time_t, message,amount,result))
    	results.append(result)
     try:   
 	cur.executemany("INSERT IGNORE INTO Transactions VALUE (%s,%s,%s,%s,%s,%s,%s,%s,%s)",transactions)
 	db.commit()
     except: 
	db.rollback()
     db.close()
     return results    

def getting_flag (partition):
    db = sqlcon.connect(user='', password='',
                        host='.compute-1.amazonaws.com',
                        database='VenmoDB')
    users_list = []
    for x in partition:
	users = rec_user(x['from_id'],x['from_username'],x['to_id'],x['to_username'],db)
	if len(users)<1:
	    check_friends(x,db)
	else:
	    users_list.extend(users)
	cursor = db.cursor()
	try:
	    cursor.executemany(users_list)
            db.commit()
	except:
	    db.rollback
	db.close()
	return partition

# To submit script:
# $SPARK_HOME/bin/spark-submit --master spark://host:7077 --executor-memory 6G spark_batch.py
if __name__ == "__main__":
    sc = SparkContext(appName="Venmo")
    read_rdd = sc.textFile("s3a://venmo-json/2016*")
    cleaned_rdd = read_rdd.map(lambda x: extract_data(x)).filter(lambda x: filter_nones(x)) # clean json data
       
    transactions_to_write = cleaned_rdd.mapPartitions(getting_flag)
    written_transactions = cleaned_rdd.mapPartitions(write_transactions)
	
    print(transactions_to_write.count(), written_transactions.count())

