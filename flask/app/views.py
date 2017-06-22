from app import app
import MySQLdb
from flask import jsonify
from flask import render_template, request
import copy
import time
import datetime
from  collections import Counter
import redis


@app.route('/')
@app.route('/transactions/')
@app.route('/friends/')
def enter ():
	return render_template("enter.html")

@app.route('/transactions/<int:user>')

def get_tran_data(user):
	db = MySQLdb.connect(host="ec2-54-158-19-194.compute-1.amazonaws.com", user="venmo", passwd="pass", db="VenmoDB")
	cursor = db.cursor()
	query_string = "SELECT * FROM Transactions WHERE ID1={user} OR ID2={user} ORDER BY Time DESC limit 20".format(user=user)
	response = cursor.execute(query_string)
	response = cursor.fetchall()
	response_list = []
        for val in response:
        	response_list.append(val)
	jsonresponse = [{"Time": x[5], "From": x[2], "To": x[4], "Message":x[6], 
			 "Amount":  x[7], "Verified": "Verified" if x[8] else "Not verified"} for x in response_list]
	query_string = "SELECT * FROM Users WHERE ID={user}".format(user=user)
	response = cursor.execute(query_string)
	response = cursor.fetchall()
	cursor.close()
        if len(response)>0:
                User = [{"Name": response[0][1]}]
        else:
                User = [{"Name": "user"}]
	return render_template("transactions.html", output = jsonresponse, User = User)
	
@app.route('/friends/<int:user>')

def get_friends_data(user):
	db = MySQLdb.connect(host="ec2-54-158-19-194.compute-1.amazonaws.com", user="venmo", passwd="pass", db="VenmoDB")
        cursor = db.cursor()
        query_string = "SELECT F.ID2,U.FullName FROM Friends F JOIN Users U ON F.ID2=U.ID WHERE F.ID1={user} limit 7".format(user=user)
        response = cursor.execute(query_string)
        response = cursor.fetchall()
        response_list = []
	response_list_FN = []
        for val in response:
                response_list.append(val[0])
		response_list_FN.append(val[1])

        friends_lists=[]
        for friend in response_list:
               query_string_ff = "SELECT F.ID2,U.FullName FROM Friends F JOIN Users U ON F.ID2=U.ID WHERE F.ID1={user} limit 7".format(user=friend)
               response = cursor.execute(query_string_ff)
               response = cursor.fetchall()
               response_listf = []
               for val in response:
                       response_listf.append(val[1])
               friends_lists.append(response_listf)

        jsonresponse = [{"Friend": str(response_list_FN[i]).strip('[]'),"FriendID":response_list[i], 
			"Friends": str(friends_lists[i]).strip('[]').replace("\'","") } for i in range(0,len(response_list))]
        
        query_string = "SELECT * FROM Users WHERE ID={user}".format(user=user)
        response = cursor.execute(query_string)
        response = cursor.fetchall()
        cursor.close()
	if len(response)>0:
        	User = [{"Name": response[0][1]}]
	else:
		User = [{"Name": "user"}]
	return render_template("friends.html", output = jsonresponse, User = User)

@app.route('/statistics')

def statistica():
    
    rediska = redis.StrictRedis(host='ec2-34-207-202-197.compute-1.amazonaws.com', port=6379, db=0)

    all_t = []
    av_t = []

    st = time.time()
    
    for i in range(20,-1,-1):

    	ts = datetime.datetime.fromtimestamp(st-i).strftime('%Y-%m-%d %H:%M:%S')
    
    	listik = rediska.lrange(ts,0,-1)
    	all = len(listik)
    	c = Counter(listik)
    	trues = c['True']

    	all_t.append([st-i,all])
    	av_t.append([st-i,trues])
	
    return jsonify(all_t = all_t, av_t = av_t)

@app.route('/atvperformance')

def index():
    return render_template('system_perform.html')

@app.route('/potential_fraud')

def fraud_detection ():
	rediska_from = redis.StrictRedis(host='ec2-34-207-202-197.compute-1.amazonaws.com', port=6379, db=1)
	rediska_to = redis.StrictRedis(host='ec2-34-207-202-197.compute-1.amazonaws.com', port=6379, db=2)
	from_users = []
    	to_users = []

	st = time.time()
	for i in range(5,-1,-1):
		ts = datetime.datetime.fromtimestamp(st-i*60).strftime('%Y-%m-%d %H:%M')
		
		from_list = rediska_from.lrange(ts,0,-1)
		to_list = rediska_to.lrange(ts,0,-1)

		from_users.extend(from_list)
		to_users.extend(to_list)
	
	from_c = Counter(from_users).most_common(20)
	to_c = Counter(to_users).most_common(20)

        db = MySQLdb.connect(host="ec2-54-158-19-194.compute-1.amazonaws.com", user="venmo", passwd="pass", db="VenmoDB")
        cursor = db.cursor()
        jsonresponse_from = [{"UserName": int(user[0]),"Number":int(user[1]) } for user in from_c]
        jsonresponse_to = [{"UserName": int(user[0]),"Number":int(user[1]) } for user in to_c] 
	return render_template("fraud.html", from_out = jsonresponse_from,to_out = jsonresponse_to)
