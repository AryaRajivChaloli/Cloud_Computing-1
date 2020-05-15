from flask import Flask, request , jsonify
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.dialects.mysql import INTEGER
import requests
import re
import constants
import datetime

app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///a1.db'

# Tables Creation : 
db = SQLAlchemy(app)
UnsignedInt = db.Integer()
UnsignedInt = UnsignedInt.with_variant(INTEGER(unsigned=True), 'sqlite')

class Rides(db.Model):
	rideId = db.Column(UnsignedInt,primary_key = True,autoincrement = True)
	created_by = db.Column(db.String, nullable = False)
	timestamp = db.Column(db.String, nullable = False)
	source = db.Column(db.Integer, nullable = False)
	destination = db.Column(db.Integer, nullable = False)
	users_list = db.Column(db.String)

class Userrides(db.Model):
	id = db.Column(db.Integer,primary_key = True)
	rideId = db.Column(UnsignedInt,nullable=False)
	username = db.Column(db.String,nullable=False)
db.create_all()

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# API to Write Database :

@app.route('/api/v1/db/write',methods = ['POST'])
def write_to_db():
	data = request.get_json()
	op1 = data['op']
	tab1=data['table']

	if (op1=='Insert'):
		col1 = data['column']
		val1 = data['value']
		l = len(col1)
		query = 'eval(tab1)('
		for i in range(l):
			query += col1[i] + '='+ '\'' + val1[i] + '\'' + ','
		query = query.strip(',')
		query += ')'
		new_user = eval(query)
		db.session.add(new_user)
		db.session.commit()
		return jsonify({'Error' : 'insertion complete'})


	elif(op1=="Delete"):
		
		conds = data['where'].split(',')
		query = 'eval(tab1).query.filter_by('
		for i in (conds):
			x = i.split('=')
			query += x[0] +'='+'\''+x[1]+'\''+','
		query = query.strip(',')
		query += ').all()'
		del_user = eval(query)
		for du in del_user:
			db.session.delete(du)
		db.session.commit()
		return jsonify({'Error' : 'deletion complete'})
		#query = eval(tab1).query.filter_by(w='').update(dict(email=''))
	
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# API to Read Database :
	
@app.route('/api/v1/db/read',methods = ['POST'])
def read_from_db():
	data = request.get_json()
	tab1=data['table']
	col1 = data['columns']
	try :
		conds = data['where'].split(',')
		query = 'eval(tab1).query.filter_by('
		for i in conds:
			x = i.split('=')
			query += x[0] +'='+'\''+x[1]+'\''+','
		query = query.strip(',')
		query += ').with_entities('
	except :
		query = 'eval(tab1).query.with_entities('

	for i in range(len(col1)):
		query+= 'eval(tab1).'+col1[i]+','
	query = query.strip(',')
	query+= ').all()'
	read_user = eval(query)

	#read_user=Users.query.filter_by(username='shreya').with_entities(Users.username,Users.password).all()
	return jsonify(read_user)

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# API to clear db :

@app.route('/api/v1/db/clear',methods=['POST'])
def clear_db():
	if request.method != 'POST':
		return jsonify({}),405

	meta = db.metadata
	for table in reversed(meta.sorted_tables):
	#print 'Clear table %s' % table
		db.session.execute(table.delete())
	db.session.commit()
	return jsonify({}),200


#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# API to Create ride :

@app.route('/api/v1/rides',methods = ['POST'])
def create_ride():
	if request.method != 'POST':
    		return jsonify({}),405
	
	data = request.get_json()
	
	try :
		cond = "username =" + data["created_by"]
		ts = data['timestamp']
		tf = '%d-%m-%Y:%S-%M-%H'
		s = int(data['source'])
		d = int(data['destination'])
	except :
		return jsonify({"Error":"Invalid request body"}),400

	values = [item.value for item in constants.Places]
	#return jsonify(values)

	# Checking if source and destination is valid
	if(s not in values):
		return jsonify({"Error":"Invalid source"}),400
	if(d not in values):
		return jsonify({"Error":"Invalid destination"}),400
	if(s==d):
		return jsonify({"Error":"Source and destination can't be same"}),400
	
	# Checking if timestamp is valid and in proper format
	try:
		datetime.datetime.strptime(ts,tf)
	except:
		return jsonify({"Error":"Invalid timestamp. Enter valid date and time or try entering in the format DD-MM-YYYY:SS-MM-HH"}),400
		

	reply = list(requests.get("http://52.203.10.5:8080/api/v1/users").json())

	#return jsonify(reply)

	if(data["created_by"] in reply):
		ans = requests.post("http://rides:80/api/v1/db/write",json ={"op":"Insert","table":"Rides","column":["created_by","timestamp","source","destination"],"value":	[data['created_by'],data['timestamp'],data['source'],data['destination']]})
		return jsonify({}),201
			
	else:
		return jsonify({"Error":"Username does not exist"}),400


#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# API to List upcoming rides
		
	
@app.route('/api/v1/rides',methods=["GET"])
def up_rides():
	if request.method != 'GET':
    		return jsonify({}),405

    # Checking for source and destination in url
	try:
		s=request.args['source']
		d=request.args['destination']
	except:
		return jsonify({"Error":"Source and destination not provided"}),400

	s = int(s)
	d = int(d)
	values = [item.value for item in constants.Places]

	# Checking for valid source and destination
	if(s not in values):
		return jsonify({"Error":"Invalid source"}),400
	if(d not in values):
		return jsonify({"Error":"Invalid destination"}),400
	if(s==d):
		return jsonify({"Error":"Source and destination can't be same"}),400
	
	cond = "source="+ str(s) +","+"destination="+ str(d)
	#return jsonify(cond)
	reply = list(requests.post("http://rides:80/api/v1/db/read",json = {"table":"Rides","columns":["rideId","created_by","timestamp"],"where": cond }).json())
	#return jsonify(reply)

	# CHecking if time is greater than current time
	tf = '%d-%m-%Y:%S-%M-%H'
	t = datetime.datetime.now()
	ct = t.strftime(tf)
	for r in reply:
		if(datetime.datetime.strptime(r[2],tf)< datetime.datetime.strptime(ct,tf)):
			reply.remove(r)

	no_of_rows = len(reply)
	if(no_of_rows == 0):
		return jsonify({}),204
		
	else:
		output = []
		for r in reply:
			ride = {}
			ride['rideId'] = r[0]
			ride['username'] = r[1]
			ride['timestamp'] = r[2]
			output.append(ride)
		return jsonify(output),200


#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# API to get details of rides
		

@app.route('/api/v1/rides/<rideId>',methods = ['GET'])
def details_of_ride(rideId):
	if request.method != 'GET':
    		return jsonify({}),405
	
	try:
		cond = "rideId =" + rideId
	except :
		return jsonify({"Error":"RideID not provided"}),400
	
	reply1 = list(requests.post("http://rides:80/api/v1/db/read",json = {"table":"Rides","columns":["rideId","created_by","timestamp","source","destination"],"where": cond }).json())
	reply2 = list(requests.post("http://rides:80/api/v1/db/read",json = {"table":"Userrides","columns":["username"],"where": cond }).json())
	l1 = len(reply1)
	l2 = len(reply2)
	s = {}
	if(l1 != 0):
		output = []
		username = []
		for u in reply2:
			username.extend(u)
			
		for i in reply1:
			s['rideId'] = str(i[0])
			s['created_by'] = str(i[1])
			s['users'] = username
			s['timestamp'] = str(i[2])
			s['source'] = str(i[3])
			s['destination'] = str(i[4])
		
			return jsonify(s),200
		
	else:
		return jsonify({"Error":"RideId does not exist"}),400


#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# API to Join existing rides :

@app.route('/api/v1/rides/<rideId>',methods = ['POST'])
def join_ride(rideId):
	if request.method != 'POST':
		return jsonify({}),405

	try:
		cond = "rideId =" + rideId
	except :
		return jsonify({"Error":"RideID not provided"}),400

	data = request.get_json()

	try:
		cond1 = "username =" + data['username']
	except :
		return jsonify({"Error":"Username not provided"}),400

	# Reading db for rides with relevant ride id:
	cond = "rideId =" + rideId
	reply1 = list(requests.post("http://rides:80/api/v1/db/read",json = {"table":"Rides","columns":["rideId"],"where": cond }).json())
	if(len(reply1) == 0):
		return jsonify({"Error":"Ride Id doesnt exist"}),400

	#cond1 = "username =" + data['username']
	reply2 = list(requests.get("http://52.203.10.5:8080/api/v1/users").json())

	if(data['username'] not in reply2):
		return jsonify({"Error":"Username doesnt exist"}),400
	reply3 = list(requests.post("http://rides:80/api/v1/db/read",json = {"table":"Rides","columns":["created_by"],"where": cond }).json())
	cu = str(reply3[0]).strip('[').strip(']').strip('\'')
	if(cu==data['username']):
		return jsonify({"Error":"Ride creator can't join ride"}),400
	
	reply4 = requests.post("http://rides:80/api/v1/db/read",json = {"table":"Userrides","columns":["username"],"where": cond }).json()
	flag =1
	for i in reply4:
		#return jsonify(i)
		if(data['username']==i[0]):
			flag=0
	if(flag):
		ans = requests.post("http://rides:80/api/v1/db/write",json ={"op":"Insert","table":"Userrides","column":["rideId","username"],"value":[rideId,data['username']]})
		return jsonify({}),200
	else:
		return jsonify({"Error":"User already present in the ride"}),400

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# API to delete rides :
	
@app.route('/api/v1/rides/<rideId>', methods = ['DELETE'])	
def delete_ride(rideId):
	if request.method != 'DELETE':
    		return jsonify({}),405
	try:
		cond = "rideId =" + rideId
	except :
		return jsonify({"Error":"RideID not provided"}),400

	reply = list(requests.post("http://rides:80/api/v1/db/read",json = {"table":"Rides","columns":["rideId"],"where": cond }).json())
	l = len(reply)
	if(l == 1):
		ans = requests.post("http://rides:80/api/v1/db/write",json ={"op":"Delete","table":"Rides","where":cond}).json()
		ans = requests.post("http://rides:80/api/v1/db/write",json ={"op":"Delete","table":"Userrides","where":cond}).json()
		return jsonify({}),200
	else:
		return jsonify({"Error":"Invalid Ride id"}),400







if __name__=='__main__':
	app.run(debug=True,host="0.0.0.0",port=80)
