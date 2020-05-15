from flask import Flask, request, flash, url_for, redirect, render_template, jsonify, request, abort
from flask_sqlalchemy import SQLAlchemy
import csv
import json
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
class table_count(db.Model):
    __tablename__ = 'table_count'
    count = db.Column(db.Integer, primary_key=True)

# Create the table
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

    # Make a request to the orchestrator db operations
    url = 'http://52.87.52.143:80/api/v1/db/read'
    url1 = 'http://52.87.52.143:80/api/v1/db/write'

    # Check if user doesn't exist
    # Return 400 if user doesn't exist
    param1 = {"table": "user", "column":"email", "value":name}
    r1 = requests.get(url, json=param1)
    
    if (r1.status_code == 204):
        return "User doesn't exist", 400

    # Delete the user by making a call to the db write in orchestrator
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

    # Make a request to the orchestrator db operations
    url = 'http://52.87.52.143:80/api/v1/db/read'
    url1 = 'http://52.87.52.143:80/api/v1/db/write'

    #check if user doesn't exist
    param1 = {"table": "user", "column":"email", "value":user}
    r1 = requests.get(url, json=param1)
    
    if (r1.status_code == 204):
        # User doesn't exist, can be created
        # After validating the length of the password and checking if it is hexadecimal
        if len(pwd) != 40:
            abort(400)
        try:
            sha_int = int(pwd, 16)
        except ValueError:
            abort(400)

        # Write to the db
        param2 = {"operation":"insert", "table": "user", "email":user, "pwd":pwd}
        r2 = requests.post(url1, json=param2)
        if r2.status_code == 200:
            return "User added", 201
        else:
            return {},r2.status_code
    else:
        # User already exists
        return "User already exists", 400

@app.route('/api/v1/db/read_all_users', methods=['POST'])
def read_all_users():
    url = 'http://52.87.52.143:80/api/v1/db/read'
    param1 = {"table": "user", "column":"all", "value":"*"}
    r1 = requests.get(url, json=param1)

    if len(r1.json()):
        return jsonify(r1.json()['output']),200
    else:
        return {}, 204


@app.route("/api/v1/users",methods=["GET"])
def list_all_users():
    count_url = 'http://users:80/api/v1/db/increment_count'
    count_resp = requests.get(count_url)

    if request.method != 'GET':
        return {},405

    url = 'http://users:80/api/v1/db/read_all_users'

    r1 = requests.post(url)
    try:
        a = r1.json()
        b = []
        for i in a:
            b.append(i['email'])
        return jsonify(b),200
    except:
        return {},204
     

if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0',port=80)
