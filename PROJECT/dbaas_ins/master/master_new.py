#!/usr/bin/env python

#importing the necessary libraries
import csv
import re 
import json
import datetime
from flask import Flask, request, flash, url_for, redirect, render_template
from flask import Flask, render_template,jsonify,request,abort
from flask import Flask, Response
import string 
import ast
import requests
import ast
from sqlalchemy import create_engine,DateTime
from sqlalchemy.exc import IntegrityError
import requests

from flask_sqlalchemy import SQLAlchemy
import os

from flask_sqlalchemy import SQLAlchemy
from flask import Flask

from sqlalchemy import orm
from flask import current_app
from flask_sqlalchemy import SQLAlchemy, get_state

# Start flask app
app = Flask(__name__)

# Set the data base uri for this connection
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///m.db'
db = SQLAlchemy(app)

engine = create_engine(app.config['SQLALCHEMY_DATABASE_URI'])

from sqlalchemy import *
from sqlalchemy import create_engine, ForeignKey
from sqlalchemy import Column, Date, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref
from sqlalchemy.orm import sessionmaker, Session


# Initialise the necessary tables
# User table - stores details of each user
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

# Used to keep track of number of requests
class table_count(db.Model):
    __tablename__ = 'table_count'
    count = db.Column(db.Integer, primary_key=True)
    
    def __init__(self, count):
        self.count = count

# Ride table - stores all the rides created
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

# Table to store a mapping of which user is part of which ride
class table_ride_user_map(db.Model):
    __tablename__ = 'ride_user_map'
    ride_id = db.Column(db.Integer,primary_key=True)
    user_id = db.Column(db.String(120),primary_key=True)

    def __init__(self, ride_id, user_id):
        self.ride_id = ride_id
        self.user_id = user_id

# Create the above tables  
db.create_all()

from kazoo.client import KazooClient
from kazoo.client import KazooState
import time
import sys
import os
from kazoo.exceptions import ConnectionLossException
from kazoo.exceptions import NoAuthException

# Establishing a connection with the zookeeper server
# Here the name of the container running zookeeper is 'zoo' hence the hostname is 'zoo'
zk = KazooClient(hosts='zoo:2181')

# Adding a listener function to check for changes in state (Lost, connected, suspended)
def my_listener(state):
    if state == KazooState.LOST:
        print("The session was lost")
    elif state == KazooState.SUSPENDED:
        print("Handle being disconnected from Zookeeper")
    else:
        print("Handle being connected/reconnected to Zookeeper")

zk.add_listener(my_listener)

zk.start()

import subprocess
import docker


client = docker.from_env()
cid=''

for container in client.containers.list():
    var = container.attrs['Config']['Env'][0]
    res = var.split(',')[0].split('=')[1]
    if "master" in res:
        cid = container.attrs['State']['Pid']
    
# Get the PID of the current process
res = cid
curr_pid = res
res = str(curr_pid)
res = bytes(res, 'utf-8')

# Creating an ephemeral node as a child to the main '/election' node
# The data of this node is the current PID
# The node is also sequential so that the path is different for each node
curr_path = zk.create("/election/node_", res, ephemeral=True, sequence=True)

# Set a data watch on the parent
# if the data changes, this function checks if it is a master or slave
@zk.DataWatch("/election")
def watch_election_node(data, stat):
    new_master_pid = (data.decode("utf-8"))
    '''
    if (curr_pid == new_master_pid):
        print("I am the master !!!!!!!!!!!!!!!!!!!!!!! ")
        
    else:
        print("I am the  slave !!!!!!!!!!!!!!!!!!!!!!! ")
        #os.environ["WT"] = "slave"
    '''
new_noc = int(os.environ["NOC"]) + 1
os.environ["NOC"] = str(new_noc)

import pika
import time
import os

if(os.environ["WT"]=='master'):
    # Establish connection with RabbitMQ server, hostname is the name of the container running RabbitMQ
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='rmq',heartbeat=1600))
    channel = connection.channel()
    # Create exchange type = fanout to route message to all the queues
    channel.exchange_declare(exchange='logs', exchange_type='fanout')

    def callback(ch, method, properties, body):

        data=[]
        data.append(json.loads(body.decode('utf-8')))
        
        # Send the message to the sync queue so that all the slaves can then see it and update their own db's
        channel.basic_publish(
				            exchange='logs',
				            routing_key='SyncQ',
				            body=str(data)
				            
          					  )

        from sqlalchemy.exc import IntegrityError
        try:
            # Make the required db operations - insert/delete/update/clear
            # Check the table to which the operation has to be carried out
            if data[0]['operation']=="insert":
                if data[0]['table']=="user":
                    user = table_user(data[0]['email'], data[0]['pwd'])
                    db.session.add(user)
                    try:
                        db.session.commit()
                    except:
                        db.session.rollback()

                if data[0]['table']=="ride":
                    ride = table_ride(data[0]['id'], data[0]['start_time'], data[0]['source'], data[0]['destination'])
                    db.session.add(ride)
                    db.session.commit()
                    return "ride_success"

                if data[0]['table']=="ride_user_map":
                    ride_user_map = table_ride_user_map(data[0]['ride_id'], data[0]['user_id'])
                    db.session.add(ride_user_map)
                    db.session.commit()
                    return "ride_user_map_success"
                    

            if data[0]['operation']=="delete":
                if data[0]['element']=="user":
                    val = data[0]['value']
                    users = table_user.query.filter_by(email=val).all()
                    for user in users:
                        db.session.delete(user)
                    try:
                        db.session.commit()
                    except:
                        db.session.rollback()

                if data[0]['element']=="ride":
                    val = data[0]['value']
                    users = table_ride_user_map.query.filter_by(ride_id=val).all()
                    for user in users:
                        db.session.delete(user)
                    db.session.commit()
                    users = table_ride.query.filter_by(ride_id=val).all()
                    for user in users:
                        db.session.delete(user)
                    db.session.commit()

            if data[0]['operation']=="update":
                if data[0]['table']=="user":
                    u_id = data[0]["id"]
                    val = data[0]["pwd"]
                    user = table_user.query.filter_by(email=u_id).first()
                    user.pwd = val
                    try:
                        db.session.commit()
                    except:
                        db.session.rollback()

                if data[0]['table']=="ride":
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

            if data[0]['operation']=="clear":
                users = table_user.query.all()
                rides = table_ride.query.all()
                ride_users = table_ride_user_map.query.all()
               
                for ride_user in ride_users:
                    db.session.delete(ride_user)
                    try:
                        db.session.commit()
                    except:
                        db.session.rollback()

                for ride in rides:
                    db.session.delete(ride)
                    try:
                        db.session.commit()
                    except:
                        db.session.rollback()

                for user in users:
                    db.session.delete(user)
                    try:
                        db.session.commit()
                    except:
                        db.session.rollback()
                return {},200
        except IntegrityError:
            return {},400

        time.sleep(body.count(b'.'))
        # Send the acknowledgement since the write operations were completed
        ch.basic_ack(delivery_tag=method.delivery_tag)

    # Consume the messages published on the write queue, perform the db operations through the callback 
    channel.basic_consume(queue='writeq', on_message_callback=callback)
    channel.start_consuming()
    
elif(os.environ["WT"]=="slave"):
    # Establish connection with RabbitMQ server, hostname is the name of the container running RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rmq',heartbeat=1600))
    channel = connection.channel()

    client = docker.from_env()
    for container in client.containers.list():
        if container.name!='master' and container.name!='orchestrator' and container.name!='zoo' and container.name!='rmq' and container.name!='slave':
            
            # If the container is not the initial 5 containers that were running, and is a newly created slave
            # Then start the syncing by receiving all the messages in the syncQ and performing the db operations
            def copy(ch, method, props, body):
                data=[]
                data.append(json.loads(body.decode('utf8').replace("'", '"')))

                # Perform the db operations
                from sqlalchemy.exc import IntegrityError
                try:
                    if data[0]['operation']=="insert":
                        
                        if data[0]['table']=="user":
                            user = table_user(data[0]['email'], data[0]['pwd'])
                            db.session.add(user)
                            try:
                                db.session.commit()
                                return "user_success"
                            except:
                                db.session.rollback()
                                return {},400
                                
                        if data[0]['table']=="ride":
                            ride = table_ride(data[0]['id'], data[0]['start_time'], data[0]['source'], data[0]['destination'])
                            db.session.add(ride)
                            db.session.commit()
                            return "ride_success"
                            
                        if data[0]['table']=="ride_user_map":
                            ride_user_map = table_ride_user_map(data[0]['ride_id'], data[0]['user_id'])
                            db.session.add(ride_user_map)
                            db.session.commit()
                            return "ride_user_map_success"


                    if data[0]['operation']=="delete":
                        if data[0]['element']=="user":
                            val = data[0]['value']
                            users = table_user.query.filter_by(email=val).all()
                            for user in users:
                                db.session.delete(user)
                            try:
                                db.session.commit()
                            except:
                                db.session.rollback()
                            return "user_deleted"
                            
                        if data[0]['element']=="ride":
                            val = data[0]['value']
                            users = table_ride_user_map.query.filter_by(ride_id=val).all()
                            for user in users:
                                db.session.delete(user)
                            db.session.commit()
                            users = table_ride.query.filter_by(ride_id=val).all()
                            for user in users:
                                db.session.delete(user)
                            db.session.commit()
                            return "ride_deleted"

                    if data[0]['operation']=="update":
                        if data[0]['table']=="user":
                            u_id = data[0]["id"]
                            val = data[0]["pwd"]
                            user = table_user.query.filter_by(email=u_id).first()
                            user.pwd = val
                            try:
                                db.session.commit()
                            except:
                                db.session.rollback()
                            return "user_updated"
                            
                        if data[0]['table']=="ride":
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
                            
                    if data[0]['operation']=="clear":
                        users = table_user.query.all()
                        rides = table_ride.query.all()
                        ride_users = table_ride_user_map.query.all()
                       
                        for ride_user in ride_users:
                            db.session.delete(ride_user)
                            try:
                                db.session.commit()
                            except:
                                db.session.rollback()

                        for ride in rides:
                            db.session.delete(ride)
                            try:
                                db.session.commit()
                            except:
                                db.session.rollback()

                        for user in users:
                            db.session.delete(user)
                            try:
                                db.session.commit()
                            except:
                                db.session.rollback()
                        return {},200
                       
                except IntegrityError:
                    return {},400
                    

                return "failure",400


                time.sleep(body.count(b'.'))
                # Send ack as the operations were successful
                ch.basic_ack(delivery_tag=method.delivery_tag)

            # Consume from the copyQ
            channel.basic_consume(queue='copyq', on_message_callback=copy)

    channel.exchange_declare(exchange='logs', exchange_type='fanout')
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue

    channel.queue_bind(exchange='logs', queue=queue_name)


    def callback(ch, method, properties, body):
        data= (json.loads(body.decode('utf-8').replace("'", '"')))
        
        # Perform the db operations
        from sqlalchemy.exc import IntegrityError
        try:
            if data[0]['operation']=="insert":
                if data[0]['table']=="user":
                    user = table_user(data[0]['email'], data[0]['pwd'])
                    db.session.add(user)
                    try:
                        db.session.commit()
                        return "user_success"
                    except:
                        db.session.rollback()
                        return {},400

                if data[0]['table']=="ride":
                    ride = table_ride(data[0]['id'], data[0]['start_time'], data[0]['source'], data[0]['destination'])
                    db.session.add(ride)
                    db.session.commit()
                    return "ride_success"
                if data[0]['table']=="ride_user_map":
                    ride_user_map = table_ride_user_map(data[0]['ride_id'], data[0]['user_id'])
                    db.session.add(ride_user_map)
                    db.session.commit()
                    return "ride_user_map_success"
                    

            if data[0]['operation']=="delete":
                if data[0]['element']=="user":
                    val = data[0]['value']
                    users = table_user.query.filter_by(email=val).all()
                    for user in users:
                        db.session.delete(user)
                    try:
                        db.session.commit()
                    except:
                        db.session.rollback()
                    return "user_deleted"
                if data[0]['element']=="ride":
                    val = data[0]['value']
                    users = table_ride_user_map.query.filter_by(ride_id=val).all()
                    for user in users:
                        db.session.delete(user)
                    db.session.commit()
                    users = table_ride.query.filter_by(ride_id=val).all()
                    for user in users:
                        db.session.delete(user)
                    db.session.commit()
                    return "ride_deleted"

            if data[0]['operation']=="update":
                if data[0]['table']=="user":
                    u_id = data[0]["id"]
                    val = data[0]["pwd"]
                    user = table_user.query.filter_by(email=u_id).first()
                    user.pwd = val
                    try:
                        db.session.commit()
                    except:
                        db.session.rollback()
                    return "user_updated"
                if data[0]['table']=="ride":
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
            if data[0]['operation']=="clear":
                users = table_user.query.all()
                rides = table_ride.query.all()
                ride_users = table_ride_user_map.query.all()
               
                for ride_user in ride_users:
                    db.session.delete(ride_user)
                    try:
                        db.session.commit()
                    except:
                        db.session.rollback()

                for ride in rides:
                    db.session.delete(ride)
                    try:
                        db.session.commit()
                    except:
                        db.session.rollback()

                for user in users:
                    db.session.delete(user)
                    try:
                        db.session.commit()
                    except:
                        db.session.rollback()
                return {},200
             
        except IntegrityError:
            return {},400
            

        return "failure",400

    # This queue will be persisted to the disk
    # persistent messages can be recovered
    channel.queue_declare(queue='rpc_queue',durable=True)

    # Perform the db read operations
    def callback1(body):
        data=[]

        data.append(json.loads(body.decode('utf8').replace("'", '"')))
        
        col = data[0]["column"]
        val = data[0]["value"]
        catalog = {}
        catalog['output'] = []

        if data[0]['table'] == "user":
            if col == "email":
                information = table_user.query.filter_by(email=val).all()
                for i in information:
                    op = {}
                    op['email'] = i.email
                    op['pwd'] = i.pwd
                    catalog['output'].append(op)
            if col == "pwd":
                information = table_user.query.filter_by(pwd=val).all()
                for i in information:
                    op = {}
                    op['email'] = i.email
                    op['pwd'] = i.pwd
                    catalog['output'].append(op)
            
            if col == "all":
                information1 = table_user.query.all()
                for i in information1:
                    op = {}
                    op['email'] = i.email
                    catalog['output'].append(op)
                

        elif data[0]['table'] == "ride":
            if col == "ride_id":
                information = table_ride.query.filter_by(ride_id=val).all()
                for i in information:
                    op = {}
                    op['ride_id'] = i.ride_id
                    op['start_time'] = i.start_time
                    op['source'] = i.source
                    op['destination'] = i.destination
                    catalog['output'].append(op)
            if col == "start_time":
                information = table_ride.query.filter_by(start_time=val).all()
                for i in information:
                    op = {}
                    op['ride_id'] = i.ride_id
                    op['start_time'] = i.start_time
                    op['source'] = i.source
                    op['destination'] = i.destination
                    catalog['output'].append(op)
            if col == "source":
                information = table_ride.query.filter_by(source=val).all()
                for i in information:
                    op = {}
                    op['ride_id'] = i.ride_id
                    op['start_time'] = i.start_time
                    op['source'] = i.source
                    op['destination'] = i.destination
                    catalog['output'].append(op)
            if col == "destination":
                information = table_ride.query.filter_by(destination=val).all()
                for i in information:
                    op = {}
                    op['ride_id'] = i.ride_id
                    op['start_time'] = i.start_time
                    op['source'] = i.source
                    op['destination'] = i.destination
                    catalog['output'].append(op)
            if col == "get_len_ride_table":
                op = []
                info = len(table_ride.query.all())
                op.append(info)
                
                catalog['output'].append(op)
                
                   
            if col == "get_prev_ride_id":
                op = []
                info = table_ride.query.all()
                rid = {'ride_id':'-1'}
                for i in info:
                    rid['ride_id'] = i.ride_id

                a = []
                a.append(rid)
                catalog['output'].append(a)

        elif data[0]['table'] == "ride_user_map":
            if col == "ride_id":
                information = table_ride_user_map.query.filter_by(ride_id=val).all()
            if col == "user_id":
                information = table_ride_user_map.query.filter_by(user_id=val).all()
            for i in information:
                op = {}
                op['ride_id'] = i.ride_id
                op['user_id'] = i.user_id
                catalog['output'].append(op)
        else:
            return {},400
            
        if len(catalog['output']):
            return json.dumps(catalog)
        else:
            return {},204       

    def on_request(ch, method, props, body):
        response = callback1(body)
        ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = props.correlation_id),body=str(response))
        ch.basic_ack(delivery_tag=method.delivery_tag)

    
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='rpc_queue', on_message_callback=on_request)
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    channel.start_consuming()

if __name__ == '__main__':  
    app.debug=True
    app.run(host='0.0.0.0',port=80)
