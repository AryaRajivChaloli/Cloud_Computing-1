from flask import Flask, request , jsonify
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.dialects.mysql import INTEGER
import requests
import re
import datetime

app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///a1.db'

# Tables Creation : 
db = SQLAlchemy(app)
UnsignedInt = db.Integer()
UnsignedInt = UnsignedInt.with_variant(INTEGER(unsigned=True), 'sqlite')

class Users(db.Model):
	username = db.Column(db.String,primary_key = True, nullable=False)
	password = db.Column(db.String, nullable = False)

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

# API to Add User :

@app.route('/api/v1/users',methods = ['PUT'])
def add_user():
	if request.method != 'PUT':
    		return jsonify({}),405
	data = request.get_json()

	try :
		cond = "username =" + data["username"]
		reply = list(requests.post("http://users:80/api/v1/db/read",json = {"table":"Users","columns":["username"],"where": cond }).json())
	
	except :
		return jsonify({"Error":" Invalid request body"}),400

	l=len(reply)
	if(l==0):
		#k=re.compile(1
		if(len(data['password'])==40):
			m=re.findall("[a-fA-F0-9]{40}",data['password']) 
			#return jsonify(m)
			if(len(m)!=0 and len(m[0])==40 ):
				ans = requests.post("http://users:80/api/v1/db/write",json ={"op":"Insert","table":"Users","column":["username","password"],"value":[data['username'],data['password']]})
				return jsonify({}),201
			else:
				return jsonify({"Error":"Password not in SHA1 hash hex format"}),400
		else:
			return jsonify({"Error":"Password not in SHA1 hash hex format"}),400
	else:
		return jsonify({"Error":"Username exists"}),400


#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# API to Remove User :

@app.route('/api/v1/users/<string:username>',methods = ['DELETE'])
def remove_user(username):
	if request.method != 'DELETE':
    		return jsonify({}),405

	cond = "username =" + username
	reply = list(requests.post("http://users:80/api/v1/db/read",json = {"table":"Users","columns":["username"],"where": cond }).json())
	l=len(reply)
	#return jsonify(l)
	if(l==1):
		rem_u = requests.post("http://users:80/api/v1/db/write",json ={"op":"Delete","table":"Users","where":cond}).json()

		return jsonify({}),200
	else:
		return jsonify({"Error":"Username does not exist"}),400



#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# API to list all users :

@app.route('/api/v1/users',methods = ['GET'])
def list_all_users():
	if request.method != 'GET':
    		return jsonify({}),405

	
	reply = requests.post("http://users:80/api/v1/db/read",json = {"table":"Users","columns":["username"]}).json()
	l=len(reply)
	#return jsonify(l)
	if(l>0):
		output =[]
		for r in reply:
			output.append(r[0])

		return jsonify(output),200
	else:
		return jsonify({}),204







if __name__=='__main__':
	app.run(debug=True,host="0.0.0.0",port=80)
