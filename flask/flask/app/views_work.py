from app import app
import MySQLdb
from flask import jsonify
from pandas import read_sql
#from flask_table import Table, Col
#from flask import request
from flask import render_template, request
import copy
#db = MySQLdb.connect(host="ec2-54-158-19-194.compute-1.amazonaws.com", user="venmo", passwd="pass", db="VenmoDB")
#cursor = db.cursor()

#class ItemTable(Table):
#	Time = Col('Time')
#	User1 = Col('From')
#	User2 = Col('To')
#	Amount = Col('$')
#	Message = Col('Message')
#	Verified = Col('Verified')

@app.route('/')
@app.route('/index')
def index():
	return "Hello, World!"

@app.route('/transactions/<int:user>')

def get_tran_data(user):
	db = MySQLdb.connect(host="ec2-54-158-19-194.compute-1.amazonaws.com", user="venmo", passwd="pass", db="VenmoDB")
	cursor = db.cursor()
	#user2 = 10
	#request = "SELECT * FROM Transaction_NT limit %(user)d"
	#response = cursor.execute(request,params={'user': user})
	query_string = "SELECT * FROM Transactions WHERE ID1={user} OR ID2={user} ORDER BY Time DESC".format(user=user)
	#query_string = "SELECT * FROM Transaction_NT LIMIT 10"
	#return query_string 
	response = cursor.execute(query_string)
	response = cursor.fetchall()
	cursor.close()
	#db.close()
	response_list = []
        for val in response:
        	response_list.append(val)
	jsonresponse = [{"Time": x[5], "From": x[2], "To": x[4], "Message":x[6], 
			 "Amount":  x[7], "Verified": "Verified" if x[8] else "Not verified"} for x in response_list]
        #table = ItemTable(jsonrsponse)
	#print(table.__html__())
	return render_template("transactions.html", output=jsonresponse)
	#return jsonify(transactions=jsonresponse)

@app.route('/friends/<int:user>')

def get_friends_data(user):
	db = MySQLdb.connect(host="ec2-54-158-19-194.compute-1.amazonaws.com", user="venmo", passwd="pass", db="VenmoDB")
        cursor = db.cursor()
        query_string = "SELECT * FROM Friends WHERE ID1={user}".format(user=user)
        response = cursor.execute(query_string)
        response = cursor.fetchall()
        response_list = []
	response_list_FN = []
        for val in response:
                response_list.append(val[1])
		query_string = "SELECT FullName FROM Users WHERE ID={user}".format(user=val[1])
	        response_name = cursor.execute(query_string)
        	response_name = cursor.fetchall()
		response_list_FN.append(response_name[0][0])

        friends_lists=[]
        for friend in response_list:
               query_string_ff = "SELECT * FROM Friends WHERE ID1={user}".format(user=friend)
               response = cursor.execute(query_string_ff)
               response = cursor.fetchall()
               response_listf = []
               for val in response:
               	       query_string = "SELECT FullName FROM Users WHERE ID={user}".format(user=val[1])
                       response_name = cursor.execute(query_string)
                       response_name = cursor.fetchall()
                       response_listf.append(response_name[0][0])
               friends_lists.append(copy.copy(response_listf))

        jsonresponse = [{"Friend": str(response_list_FN[i]).strip('[]'), "Friends": str(friends_lists[i]).strip('[]') } for i in range(0,len(response_list))]
        #table = ItemTable(jsonrsponse)
        #print(table.__html__())
        #return render_template("transactions.html", output=jsonresponse)
        #return jsonify(friends=jsonresponse)
	return render_template("friends.html", output=jsonresponse)
