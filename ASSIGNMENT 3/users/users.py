from flask import Flask, request, flash, url_for, redirect, render_template, jsonify, request, abort
from flask_sqlalchemy import SQLAlchemy
import csv
from sqlalchemy import create_engine
import datetime
import requests
from sqlalchemy import func
import werkzeug.exceptions

# Start the flask app
app = Flask(__name__)
# Set the data base uri for this connection
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///UserDB.sqlite'
db = SQLAlchemy(app)

engine = create_engine(app.config['SQLALCHEMY_DATABASE_URI'])

# Initialise the required tables
class table_user(db.Model):
    __tablename__ = 'table_user'
    email = db.Column(db.String(120),
                      unique=True,
                      nullable=False,
                      primary_key=True)

    pwd = db.Column(db.String(80), nullable=False)

    def __init__(self, email, pwd):
        self.email = email
        self.pwd = pwd

class table_count(db.Model):
    __tablename__ = 'table_count'
    count = db.Column(db.Integer, primary_key=True)

# Create the tables
db.create_all()

try:
    exec_str = "INSERT INTO table_count(count) VALUES (0)"
    engine.execute(exec_str)
except:
    db.session.rollback()

# API to get how many http requests were made
@app.route('/api/v1/_count', methods = ['GET'])
def read_count():
    if request.method != 'GET':
        return {}, 405

    return jsonify([len(db.session.query(table_count).all())-1]),200

# API to reset how many http requests were made
@app.route('/api/v1/_count', methods = ['DELETE'])
def reset_count():
    if request.method != 'DELETE':
        return {}, 405
    
    count_obj = table_count.query.all()

    for i in count_obj:
        db.session.delete(i)
        db.session.commit()
    try:
        exec_str = "INSERT INTO table_count(count) VALUES (0)"
        engine.execute(exec_str)
    except:
        db.session.rollback()

    return {}, 200

# Internal API to increment the number of http requests
@app.route('/api/v1/db/increment_count', methods = ['GET'])   
def increment_count():
    str1=''
    val=(db.session.query(db.func.max(table_count.count)).scalar())+1

    str1=str1+'"'+str(val)+'"'
    execstr = 'INSERT INTO table_count(count) VALUES'+'('+str1+')'
    engine.execute(execstr)
    db.session.commit()
    return {},201

# API to make db write operations
@app.route('/api/v1/db/write', methods = ['POST'])
def writedb():
    data = request.get_json()

    if data['operation']=="insert":
        if data['table']=="user":
            user = table_user(data['email'], data['pwd'])
            db.session.add(user)
            db.session.commit()
            return "user_success"

    if data['operation']=="delete":
        if data['element']=="user":
            val = data['value']
            users = table_user.query.filter_by(email=val).all()
            for user in users:
                db.session.delete(user)
            db.session.commit()
            return "user_deleted"

    if data['operation']=="update":
        if data['table']=="user":
            u_id = data["id"]
            val = data["pwd"]
            user = table_user.query.filter_by(email=u_id).first()
            user.pwd = val
            db.session.commit()
            return "user_updated"
    
    return "failure",400

# API to make db read operations
@app.route('/api/v1/db/read', methods=['POST'])
def read():
    data = request.get_json()
    col = data["column"]
    val = data["value"]
    catalog = {}
    catalog['output'] = []

    if data['table'] == "user":
        if col == "email":
            information = table_user.query.filter_by(email=val).all()

        if col == "pwd":
            information = table_user.query.filter_by(pwd=val).all()

        for i in information:
            op = {}
            op['email'] = i.email
            op['pwd'] = i.pwd
            catalog['output'].append(op)

        if len(catalog['output']):
            return jsonify(catalog),200

        else:
            return {},204

    return {},400

# Register a handler to take care of "bad method" requests
@app.errorhandler(werkzeug.exceptions.MethodNotAllowed)
def handle_bad_request(e):
    # Increment the count even if it is a bad method
    count_url = 'http://users:80/api/v1/db/increment_count'
    count_resp = requests.get(count_url)
    return {}, 405


@app.route("/api/v1/users/<name>",methods=["DELETE"])
def delete_user(name):
    count_url = 'http://users:80/api/v1/db/increment_count'
    count_resp = requests.get(count_url)

    if request.method != 'DELETE':
        abort(405)

    url = 'http://users:80/api/v1/db/read'
    url1 = 'http://users:80/api/v1/db/write'

    #check if user doesn't exist
    # Return 400 if user doesn't exist
    param1 = {"table": "user", "column":"email", "value":name}
    r1 = requests.post(url, json=param1)
    
    if (r1.status_code == 204):
        return "User doesn't exist", 400

    # If the user exists, delete it
    param2 = {"operation":"delete", "element": "user", "value":name}
    r2 = requests.post(url1, json=param2)

    return "User delete successful", 200


@app.route("/api/v1/users",methods=["PUT"])
def add_user():
    count_url = 'http://users:80/api/v1/db/increment_count'
    count_resp = requests.get(count_url)

    if request.method != 'PUT':
        abort(405)
    user = request.get_json()["username"]
    pwd = request.get_json()["password"]
    url = 'http://users:80/api/v1/db/read'
    url1 = 'http://users:80/api/v1/db/write'

    # Check if user doesn't exist
    # If the user exists, return 400
    # If not the check for the correctness of the password and then add the user
    param1 = {"table": "user", "column":"email", "value":user}
    r1 = requests.post(url, json=param1)
    
    if (r1.status_code == 204):
        if len(pwd) != 40:
            abort(400)
        try:
            sha_int = int(pwd, 16)
        except ValueError:
            abort(400)
        param2 = {"operation":"insert", "table": "user", "email":user, "pwd":pwd}
        r2 = requests.post(url1, json=param2)
        if r2.status_code == 200:
            return "User added", 201
        else:
            return {},r2.status_code
    else:
        return "User already exists", 400

@app.route('/api/v1/db/read_all_users', methods=['POST'])
def read_all_users():
    catalog = {}
    catalog['output'] = []
    
    # Append the email ids
    information = table_user.query.all()
    for i in information:
        catalog['output'].append(i.email)

    if len(catalog['output']):
        return jsonify(catalog),200
    else:
        return {}, 204
    return {}, 400

@app.route("/api/v1/users",methods=["GET"])
def list_all_users():
    count_url = 'http://users:80/api/v1/db/increment_count'
    count_resp = requests.get(count_url)

    if request.method != 'GET':
        return {},405

    # Make call to internal api, return the list, if obtained
    url = 'http://users:80/api/v1/db/read_all_users'
    r1 = requests.post(url)

    if (r1.status_code == 204):
        return {}, 204

    response1 = r1.json()['output']
    return jsonify(response1), 200

@app.route('/api/v1/db/clear', methods = ['POST'])
def cleardb():
    ride_users = table_ride_user_map.query.all()
    users = table_user.query.all()
    rides = table_ride.query.all()

    # Get all the users, delete each entry
    for user in users:
        db.session.delete(user)
        db.session.commit()

    return "database cleared", 200

if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0',port=80)
