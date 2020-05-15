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
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///RideDB.sqlite'
db = SQLAlchemy(app)

engine = create_engine(app.config['SQLALCHEMY_DATABASE_URI'])

# Initialise the required tables
class table_area_code_map(db.Model):
    __tablename__ = 'area_code_map'
    area_code = db.Column('area_code', db.Integer, primary_key=True)
    area_name = db.Column('area_name', db.String)

    def __init__(self, area_code, area_name):
        self.area_code = area_code
        self.area_name = area_name

class table_count(db.Model):
    __tablename__ = 'table_count'
    count = db.Column(db.Integer, primary_key=True)
    
    def __init__(self, count):
        self.count = count

class table_ride_count(db.Model):
    __tablename__ = 'table_ride_count'
    ride_count = db.Column(db.Integer, primary_key=True)
    
    def __init__(self, ride_count):
        self.ride_count = ride_count

class table_ride_user_map(db.Model):
    __tablename__ = 'ride_user_map'
    ride_id = db.Column(db.Integer,primary_key=True)
    user_id = db.Column(db.String(120),primary_key=True)

    def __init__(self, ride_id, user_id):
        self.ride_id = ride_id
        self.user_id = user_id

# Create the tables
db.create_all()

try:
    with open('AreaNameEnum.csv', 'r') as filepointer:
        line_dict = csv.DictReader(filepointer)
        
        for i in line_dict:
         execstr = 'INSERT INTO area_code_map(area_code,area_name) VALUES ' + '(' + i['Area No'] + ',' + '"' + i['Area Name'] + '"' + ')'
         engine.execute(execstr)
except:
    db.session.rollback()

try:
    exec_str = "INSERT INTO table_count(count) VALUES (0)"
    engine.execute(exec_str)

except:
    db.session.rollback()

try:
    exec_str = "INSERT INTO table_ride_count(ride_count) VALUES (0)"
    engine.execute(exec_str)

except:
    db.session.rollback()

# API to get how many http requests were made
@app.route('/api/v1/_count', methods = ['GET'])
def read_count():
    if request.method != 'GET':
        return {}, 405

    return jsonify([len(db.session.query(table_count).all())-1]),200

# API to reset the number of http requests made
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
    if request.method != 'GET':
        return {}, 405

    str1=''
    val=(db.session.query(db.func.max(table_count.count)).scalar())+1

    str1=str1+'"'+str(val)+'"'
    execstr = 'INSERT INTO table_count(count) VALUES'+'('+str1+')'
    engine.execute(execstr)
    db.session.commit()
    return {},201

# API to get number of rides created
@app.route('/api/v1/rides/count', methods = ['GET'])
def get_ride_count():
    count_url = 'http://rides:80/api/v1/db/increment_count'
    count_resp = requests.get(count_url)

    if request.method != 'GET':
        return {}, 405

    # Make a call to the orchestrator read
    response2 = requests.get("http://52.87.52.143:80/api/v1/db/read",json={'table':'ride','column':'get_len_ride_table','value':'.'})
    return jsonify(response2.json()), 200 

@app.route('/api/v1/db/increment_ride_count', methods = ['GET'])
def increment_ride_count():
    if request.method != 'GET':
        return {}, 405

    ride_count_obj = table_ride_count.query.all()

    for i in ride_count_obj:
        i.ride_count = i.ride_count + 1
        db.session.commit()
        return jsonify([i.ride_count]), 200
    return "error", 404

# Register a handler to take care of "bad method" requests
@app.errorhandler(werkzeug.exceptions.MethodNotAllowed)
def handle_bad_request(e):
    # Increment the count even if it is a bad method
    count_url = 'http://rides:80/api/v1/db/increment_count'
    count_resp = requests.get(count_url) 
    return {}, 405


@app.route("/api/v1/rides/<rideId>", methods=["POST"])
def join_ride(rideId):
    count_url = 'http://rides:80/api/v1/db/increment_count'
    count_resp = requests.get(count_url)

    if request.method != 'POST':
        return {},405

    #read the username from request
    user = request.get_json()["username"]
    
    # Read request to the orchestrator
    url = 'http://52.87.52.143:80/api/v1/db/read'
    # Request to the users container through the load balancer
    other_url = 'http://RideShare-1310550014.us-east-1.elb.amazonaws.com/api/v1/users'

    #check if user doesn't exist
    r1 = requests.get(other_url)
    response1 = r1.json()

    if (user not in response1):
        return {}, 204

    # Check if the ride exists
    param2 = {"table": "ride", "column": "ride_id", "value": rideId}
    r2 = requests.get(url, json=param2)
    response2 = r2.json()['output']

    if (len(response2) == 0):
        # ride doesn't exist so return with no content
        return {}, 204

    param3 = {"table": "ride_user_map", "column": "ride_id", "value": rideId}

    r3 = requests.get(url, json=param3)
    response3 = r3.json()['output']

    # Ride hasn't been created by anybody, cant join it
    if (len(response3) == 0):
        return {},400

    for entry in response3:
        if (entry['user_id'] == user):
            # User is already a part if this ride
            return {}, 400

    # Now adding them to the ride
    url2 = 'http://52.87.52.143:80/api/v1/db/write'

    param4 = {
        "operation": "insert",
        "table": "ride_user_map",
        "ride_id": rideId,
        "user_id": user
    }

    r4 = requests.post(url2, json=param4)
    return {}, 200


@app.route("/api/v1/rides/<rideId>", methods=["DELETE"])
def delete_ride(rideId):
    count_url = 'http://rides:80/api/v1/db/increment_count'
    count_resp = requests.get(count_url)

    if request.method != 'DELETE':
        return {},405

    # Make requests to the orchestrator read, write apis
    url = 'http://52.87.52.143:80/api/v1/db/read'
    url1 = 'http://52.87.52.143:80/api/v1/db/write'

    #check if ride doesn't exist
    param1 = {"table": "ride_user_map", "column": "ride_id", "value": rideId}
    r1 = requests.get(url, json=param1)
    response1 = r1.json()['output']

    if (len(response1) == 0):
        return {}, 400

    #deleting all entries in ride
    param2 = {"operation": "delete", "element": "ride", "value": rideId}
    r2 = requests.post(url1, json=param2)

    return {}, 200

def validate_ts(date_text):
    try:
        # Check if the ride is not created in the past
        nowtime = datetime.datetime.now()
        present_ts = nowtime.strftime("%d-%m-%Y:%S-%M-%H")  #obj to string
        present_dt = datetime.datetime.strptime(present_ts,
                                                "%d-%m-%Y:%S-%M-%H")  #string to obj

        timestamp = datetime.datetime.strptime(date_text, '%d-%m-%Y:%S-%M-%H')

        if timestamp > present_dt:
            return 1
        else:
            return 0

    except ValueError:
        return 0


@app.route("/api/v1/rides", methods=["POST"])
def new_ride():
    count_url = 'http://rides:80/api/v1/db/increment_count'
    count_resp = requests.get(count_url)

    emailid = request.get_json()["created_by"]
    timestamp = request.get_json()["timestamp"]
    source = request.get_json()["source"]
    destination = request.get_json()["destination"]

    valid_response = validate_ts(timestamp)

    if (valid_response):    

        # Make a request to the users instance through the load balancer
        other_url = 'http://RideShare-1310550014.us-east-1.elb.amazonaws.com/api/v1/users'

        #check if user doesn't exist
        r1 = requests.get(other_url)
        if(r1.status_code == 200):
            response1 = r1.json()
        else:
            response1 = []
            return {},400
            
        if (emailid not in response1):
            return {},400

        else:
            # Get the id of the last ride
            response2 = requests.get("http://52.87.52.143:80/api/v1/db/read",json={'table':'ride','column':'get_prev_ride_id','value':'.'})
            prev_ride_id_tup = response2.json()
            prev_ride_id = prev_ride_id_tup['output'][0][0]['ride_id']

            no_of_areas = db.session.query(table_area_code_map).all()

            # Check if the given source and destination lie within the given area codes
            if (int(source) >= 1 and int(source) <= len(no_of_areas) and int(destination) >= 1 and int(destination) <= len(no_of_areas)):
                # Sending requests to create entries in the db for both the tables
                response1 = requests.post(
                    "http://52.87.52.143:80/api/v1/db/write",
                    json={
                        "table": "ride",
                        "id": int(prev_ride_id) + 1,
                        "start_time": timestamp,
                        "source": int(source),
                        "destination": int(destination),
                        "userid":emailid,
                        "operation": "insert"
                    })

                if(response1.status_code == 200):
                    response2 = requests.post(
                        "http://52.87.52.143:80/api/v1/db/write",
                        json={
                            "table": "ride_user_map",
                            "operation": "insert",
                            "ride_id": int(prev_ride_id) + 1,
                            "user_id": emailid
                        })

                    if(response2.status_code == 200):
                        return {}, 201
    return {}, 400

@app.route("/api/v1/rides", methods=["GET"])
def list_upcoming():
    count_url = 'http://rides:80/api/v1/db/increment_count'
    count_resp = requests.get(count_url)

    source = request.args['source']
    destination = request.args['destination']

    # Get all the requests that have the same source as the req source
    response_read = requests.get("http://52.87.52.143:80/api/v1/db/read",json={"table":"ride","column":"source","value":source})
    response_read_dst = requests.get("http://52.87.52.143:80/api/v1/db/read",json={"table":"ride","column":"destination","value":destination})

    ride_details = []

    if response_read.status_code == 200:
        for i in range(len(response_read.json()["output"])):
            if response_read.json()["output"][i]["destination"] == int(destination):
                # IF the destination matches the req destination, then collect the details
                det = {}
                ride_details.append(det)
                len1 = len(ride_details)
                ride_details[len1-1]["rideId"] =  response_read.json()["output"][i]["ride_id"]
                ride_details[len1-1]["timestamp"] = response_read.json()["output"][i]["start_time"]
                ride_details[len1-1]["username"] = ''

        for i in range(len(ride_details)):
            # Use the ride id of the required rides to fetch all the details
            response_read1 = requests.get("http://52.87.52.143:80/api/v1/db/read",json={"table":"ride_user_map","column":"ride_id","value":ride_details[i]["rideId"]})

            ride_details[i]["username"] = response_read1.json()["output"][0]["user_id"]

        if (len(ride_details)):
            j = len(ride_details)
            ride_details_final = []

            # only if the ride is not yet over, store its details
            for i in range(j):
                ts = validate_ts(ride_details[i]["timestamp"])
                if (ts):
                    ride_details_final.append(ride_details[i])

            return jsonify(ride_details_final)

        elif not(response_read_dst.status_code != 200):
            return {},400

        elif not(response_read.status_code == 200):
            return {},400

        else:
            return {},204
    else:
        return {},response_read.status_code

@app.route("/api/v1/rides/<string:ride_id>", methods=["GET"])
def get_details(ride_id):
    count_url = 'http://rides:80/api/v1/db/increment_count'
    count_resp = requests.get(count_url)

    try:
        ride_id = int(ride_id)

    except:
        return {},204

    # Make call to orchestrator read using the ride id given 
    response_read = requests.get("http://52.87.52.143:80/api/v1/db/read",json={"table":"ride","column":"ride_id","value":ride_id})
    ride_details = {}    

    # to get all the users part of the ride
    response_read1 = requests.get("http://52.87.52.143:80/api/v1/db/read",json={"table":"ride_user_map","column":"ride_id","value":ride_id})

    # Store all the details
    if response_read.status_code == 200:  
        ride_details["rideId"]  = response_read.json()["output"][0]["ride_id"]
        ride_details["created_by"] = response_read1.json()["output"][0]["user_id"]
        ride_details["users"] = []
        ride_details["timestamp"] = response_read.json()["output"][0]["start_time"]
        ride_details["source"] = response_read.json()["output"][0]["source"]
        ride_details["destination"] = response_read.json()["output"][0]["destination"]

        for i in range(len(response_read1.json()["output"])):
            ride_details["users"].append(response_read1.json()["output"][i]["user_id"])

        if len(ride_details.keys()):
            return jsonify(ride_details),200
        else:
            return {},204
    else:
        return {},response_read.status_code

if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0',port=80)
