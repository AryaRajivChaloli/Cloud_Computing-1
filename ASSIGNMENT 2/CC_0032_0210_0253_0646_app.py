from flask import Flask, request, flash, url_for, redirect, render_template, jsonify, request, abort
from flask_sqlalchemy import SQLAlchemy
import csv
from sqlalchemy import create_engine
import datetime
import requests
from sqlalchemy import func

# Start the flask app
app = Flask(__name__)
# Set the data base uri for this connection
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///RideShare1.sqlite'

db = SQLAlchemy(app)
engine = create_engine(app.config['SQLALCHEMY_DATABASE_URI'])

# Initialise the required tables
# Users - stores all the user details
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

#Rides - stores all the rides details
class table_ride(db.Model):
    __tablename__ = 'table_ride'
    ride_id = db.Column(db.Integer,primary_key=True)
    start_time = db.Column(db.String(120), nullable=False)
    source = db.Column(db.Integer, nullable=False)
    destination = db.Column(db.Integer, nullable=False)

    def __init__(self, ride_id,start_time, source, destination):
        self.ride_id = ride_id
        self.start_time = start_time
        self.source = source
        self.destination = destination

# Stores a mapping between the ride and all the users part of the ride
class table_ride_user_map(db.Model):
    __tablename__ = 'ride_user_map'
    ride_id = db.Column(db.Integer,db.ForeignKey('table_ride.ride_id'),primary_key=True)
    user_id = db.Column(db.String(120),db.ForeignKey('table_user.email'),primary_key=True)

    def __init__(self, ride_id, user_id):
        self.ride_id = ride_id
        self.user_id = user_id

# Stores all the area codes
class table_area_code_map(db.Model):
    __tablename__ = 'area_code_map'
    area_code = db.Column('area_code', db.Integer, primary_key=True)
    area_name = db.Column('area_name', db.String)

    def __init__(self, area_code, area_name):
        self.area_code = area_code
        self.area_name = area_name

# Create the tables
db.create_all()

try:
    with open('AreaNameEnum.csv', 'r') as filepointer:
        line_dict = csv.DictReader(filepointer)
        
        for i in line_dict:
         execstr = 'INSERT INTO area_code_map(area_code,area_name) VALUES ' + '(' + i[
             'Area No'] + ',' + '"' + i['Area Name'] + '"' + ')'
         engine.execute(execstr)
except:
    db.session.rollback()
             

# Performs the db write operations
@app.route('/api/v1/db/write', methods = ['POST'])
def writedb():
    data = request.get_json()

    if data['operation']=="insert":
        if data['table']=="user":
            user = table_user(data['email'], data['pwd'])
            db.session.add(user)
            db.session.commit()
            return "user_success"
        if data['table']=="ride":
            ride = table_ride(data['id'], data['start_time'], data['source'], data['destination'])
            db.session.add(ride)
            db.session.commit()
            return "ride_success"
        if data['table']=="ride_user_map":
            ride_user_map = table_ride_user_map(data['ride_id'], data['user_id'])
            db.session.add(ride_user_map)
            db.session.commit()
            return "ride_user_map_success"

    if data['operation']=="delete":
        if data['element']=="user":
            val = data['value']
            users = table_ride_user_map.query.filter_by(user_id=val).all()
            for user in users:
                db.session.delete(user)
            db.session.commit()
            users = table_user.query.filter_by(email=val).all()
            for user in users:
                db.session.delete(user)
            db.session.commit()
            return "user_deleted"
        if data['element']=="ride":
            val = data['value']
            users = table_ride_user_map.query.filter_by(ride_id=val).all()
            for user in users:
                db.session.delete(user)
            db.session.commit()
            users = table_ride.query.filter_by(ride_id=val).all()
            for user in users:
                db.session.delete(user)
            db.session.commit()
            return "ride_deleted"

    if data['operation']=="update":
        if data['table']=="user":
            u_id = data["id"]
            val = data["pwd"]
            user = table_user.query.filter_by(email=u_id).first()
            user.pwd = val
            db.session.commit()
            return "user_updated"
        if data['table']=="ride":
            r_id = data["id"]
            val = data["val"]
            col = data["col"]
            ride = table_ride.query.filter_by(ride_id=r_id).first()
            if col=="start_time":
                ride.start_time = val
            if col=="source":
                ride.source = val
            if col=="destination":
                ride.destination = val
            db.session.commit()
            return "ride_updated"
    
    return "failure",400

# Performs the db read operations
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

    if data['table'] == "ride":
        if col == "ride_id":
            information = table_ride.query.filter_by(ride_id=val).all()
        if col == "start_time":
            information = table_ride.query.filter_by(start_time=val).all()
        if col == "source":
            information = table_ride.query.filter_by(source=val).all()
        if col == "destination":
            information = table_ride.query.filter_by(destination=val).all()
        
        for i in information:
            op = {}
            op['ride_id'] = i.ride_id
            op['start_time'] = i.start_time
            op['source'] = i.source
            op['destination'] = i.destination
            catalog['output'].append(op)
        if len(catalog['output']):
            return jsonify(catalog),200
        else:
            return {},204


    if data['table'] == "ride_user_map":
        if col == "ride_id":
            information = table_ride_user_map.query.filter_by(
                ride_id=val).all()
        if col == "user_id":
            information = table_ride_user_map.query.filter_by(
                user_id=val).all()
        for i in information:
            op = {}
            op['ride_id'] = i.ride_id
            op['user_id'] = i.user_id
            catalog['output'].append(op)
        if len(catalog['output']):
            return jsonify(catalog),200
        else:
            return {},204

    return {},400


@app.route("/api/v1/rides/<rideId>", methods=["POST"])
def join_ride(rideId):
    if request.method != 'POST':
        return {},405

    #read the username from request
    user = request.get_json()["username"]

    url = 'http://127.0.0.1:5000/api/v1/db/read'

    #check if user doesn't exist
    param1 = {"table": "user", "column": "email", "value": user}
    r1 = requests.post(url, json=param1)
    response1 = r1.json()['output']

    if (len(response1) == 0):
        return {}, 204


    #check if ride doesn't exist
    param2 = {"table": "ride", "column": "ride_id", "value": rideId}
    r2 = requests.post(url, json=param2)
    response2 = r2.json()['output']

    if (len(response2) == 0):
        return {}, 204


    #check if user is already part of ride
    param3 = {"table": "ride_user_map", "column": "ride_id", "value": rideId}
    r3 = requests.post(url, json=param3)
    response3 = r3.json()['output']

    if (len(response3) == 0):
        return {},400

    for entry in response3:
        if (entry['user_id'] == user):
            return {}, 400

    # Now adding them to the ride
    url2 = 'http://127.0.0.1:5000/api/v1/db/write'
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
    if request.method != 'DELETE':
        return {},405

    url = 'http://127.0.0.1:5000/api/v1/db/read'
    url1 = 'http://127.0.0.1:5000/api/v1/db/write'

    #check if ride doesn't exist
    param1 = {"table": "ride_user_map", "column": "ride_id", "value": rideId}
    r1 = requests.post(url, json=param1)
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
        present_ts = nowtime.strftime("%d-%m-%Y:%S-%M-%H")   #obj to string
        present_dt = datetime.datetime.strptime(present_ts,
                                                "%d-%m-%Y:%S-%M-%H")   #string to obj
        timestamp = datetime.datetime.strptime(date_text, '%d-%m-%Y:%S-%M-%H')
        if timestamp > present_dt:
            return 1
        else:
            return 0
    except ValueError:
        return 0


@app.route("/api/v1/rides", methods=["POST"])
def new_ride():
    emailid = request.get_json()["created_by"]
    timestamp = request.get_json()["timestamp"]
    source = request.get_json()["source"]
    destination = request.get_json()["destination"]

    valid_response = validate_ts(timestamp)
    if (valid_response):
        response = requests.post("http://127.0.0.1:5000/api/v1/db/read",
                                 json={
                                     "table": "user",
                                     "column": "email",
                                     "value": emailid
                                 })
        
        if (response.status_code == 200):
            # Get the id of the last ride
            prev_ride_id_tup = db.session.query(func.max(table_ride.ride_id))
            prev_ride_id = prev_ride_id_tup[0][0]

            # Check if the given source and destination lie within the given area codes
            no_of_areas = db.session.query(table_area_code_map).all()
            if (int(source) >= 1 and int(source) <= len(no_of_areas) and int(destination) >= 1 and int(destination) <= len(no_of_areas)):
                if str(prev_ride_id_tup[0][0]) == 'None':
                    prev_ride_id = 0
                else:
                    prev_ride_id = prev_ride_id_tup[0][0]
                # Sending requests to create entries in the db for both the tables
                response1 = requests.post(
                    "http://127.0.0.1:5000/api/v1/db/write",
                    json={
                        "table": "ride",
                        "id": prev_ride_id + 1,
                        "start_time": timestamp,
                        "source": int(source),
                        "destination": int(destination),
                        "userid":emailid,
                        "operation": "insert"
                    })
                if(response1.status_code == 200):
                    response2 = requests.post(
                        "http://127.0.0.1:5000/api/v1/db/write",
                        json={
                            "table": "ride_user_map",
                            "operation": "insert",
                            "ride_id": prev_ride_id + 1,
                            "user_id": emailid
                        })
                    if(response2.status_code == 200):
                        return {}, 201
                    
    return {}, 400

@app.route("/api/v1/rides", methods=["GET"])
def list_upcoming():
    source = request.args['source']
    destination = request.args['destination']
    
    # Get all the requests that have the same source as the req source
    response_read = requests.post("http://127.0.0.1:5000/api/v1/db/read",json={"table":"ride","column":"source","value":source})
    response_read_dst = requests.post("http://127.0.0.1:5000/api/v1/db/read",json={"table":"ride","column":"destination","value":destination})
   
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
            response_read1 = requests.post("http://127.0.0.1:5000/api/v1/db/read",json={"table":"ride_user_map","column":"ride_id","value":ride_details[i]["rideId"]})
            ride_details[i]["username"] = response_read1.json()["output"][0]["user_id"]
        if (len(ride_details)):
            j = len(ride_details)
            ride_details_final = []
            for i in range(j):
                ts = validate_ts(ride_details[i]["timestamp"])
                # only if the ride is not yet over, store its details
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
    try:
        ride_id = int(ride_id)
    except:
        return {},204
    
    # Make call to read the given ride id
    response_read = requests.post("http://127.0.0.1:5000/api/v1/db/read",json={"table":"ride","column":"ride_id","value":ride_id})
    ride_details = {}
    
    # to get all the users part of the ride
    response_read1 = requests.post("http://127.0.0.1:5000/api/v1/db/read",json={"table":"ride_user_map","column":"ride_id","value":ride_id})
    
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
        
@app.route("/api/v1/users/<name>",methods=["DELETE"])
def delete_user(name):
    if request.method != 'DELETE':
        return {},400

    url = 'http://127.0.0.1:5000/api/v1/db/read'
    url1 = 'http://127.0.0.1:5000/api/v1/db/write'

    #check if user doesn't exist
    # Return 400 if user doesn't exist
    param1 = {"table": "user", "column":"email", "value":name}
    r1 = requests.post(url, json=param1)
    
    if (r1.status_code == 204):
        return {}, 400

    
    param2 = {"operation":"delete", "element": "user", "value":name}
    r2 = requests.post(url1, json=param2)

    return {}, 200



@app.route("/api/v1/users",methods=["PUT"])
def add_user():
    if request.method != 'PUT':
        return {},405
    user = request.get_json()["username"]
    pwd = request.get_json()["password"]
    url = 'http://127.0.0.1:5000/api/v1/db/read'
    url1 = 'http://127.0.0.1:5000/api/v1/db/write'

    # Check if user doesn't exist
    # If the user exists, return 400
    # If not the check for the correctness of the password and then add the user
    param1 = {"table": "user", "column":"email", "value":user}
    r1 = requests.post(url, json=param1)
    
    if (r1.status_code == 204):
        if len(pwd) != 40:
            return {},400
        try:
            sha_int = int(pwd, 16)
        except ValueError:
            return {},400
        param2 = {"operation":"insert", "table": "user", "email":user, "pwd":pwd}
        r2 = requests.post(url1, json=param2)
        if r2.status_code == 200:
            return {}, 201
        else:
            return {},r2.status_code
    else:
        return {}, 400
 

if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0')
