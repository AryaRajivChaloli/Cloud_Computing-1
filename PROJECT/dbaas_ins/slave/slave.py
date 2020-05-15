#!/usr/bin/env python
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
app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///pro.db'
db = SQLAlchemy(app)

engine = create_engine(app.config['SQLALCHEMY_DATABASE_URI'])

from sqlalchemy import *
from sqlalchemy import create_engine, ForeignKey
from sqlalchemy import Column, Date, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref
from sqlalchemy.orm import sessionmaker, Session


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
    
    def __init__(self, count):
        self.count = count
        print("Initilized count")

db.create_all()

#---------------------------- Zookeeper ------------------------
from kazoo.client import KazooClient
from kazoo.client import KazooState
#from kazoo.handlers.gevent import SequentialGeventHandler
import time
import sys
import os
from kazoo.exceptions import ConnectionLossException
from kazoo.exceptions import NoAuthException

zk = KazooClient(hosts='zoo:2181')

def my_listener(state):
    if state == KazooState.LOST:
        print("The session was lost")
    elif state == KazooState.SUSPENDED:
        print("Handle being disconnected from Zookeeper")
    else:
        print("Handle being connected/reconnected to Zookeeper")

zk.add_listener(my_listener)

zk.start()

############# Setting up a new node

##### getting the current pid

import subprocess
import docker

client = docker.from_env()
cid=''
for container in client.containers.list():
    if "slave" in container.name:
        cid = container.attrs['State']['Pid']
        

res = cid
print("The result = ", res)
curr_pid = int(res)
res = str(curr_pid)
res = bytes(res, 'utf-8')

curr_path = zk.create("/election/node_", res, ephemeral=True, sequence=True)
print("Created an ephemeral node:", curr_path)

############# Finding if current worker is master or slave

#### Setting a watch on the /election node to see if the master pid changes

@zk.DataWatch("/election")
def watch_election_node(data, stat):
    new_master_pid = int(data.decode("utf-8"))

    print("The new master:", new_master_pid)
    print("My pid:", curr_pid)
    if (curr_pid == new_master_pid):
        print("I am the master !!!!!!!!!!!!!!!!!!!!!!! ")
        os.environ["WT"] = 'master'
    else:
        print("I am the  slave !!!!!!!!!!!!!!!!!!!!!!! ")
        os.environ["WT"] = 'slave'

'''
new_noc = int(os.environ["NOC"]) + 1
os.environ["NOC"] = str(new_noc)
print("modified noc = ", os.environ["NOC"])
'''

#---------------------------------------------------------------
import pika
import time
import os

if(os.environ["WT"]=='master'):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='rmq'))
    channel = connection.channel()
    channel.exchange_declare(exchange='logs', exchange_type='fanout')
    print(' [*] Waiting for messages. To exit press CTRL+C')


    def callback(ch, method, properties, body):

        data=[]
        print(" [x] Received %r" % body)

        data.append(json.loads(body.decode('utf-8')))
        channel.basic_publish(
				            exchange='logs',
				            routing_key='SyncQ',
				            body=str(data)
				            
          					  )

        from sqlalchemy.exc import IntegrityError
        try:
            if data[0]['operation']=="insert":
                if data[0]['table']=="user":
                    user = table_user(data[0]['email'], data[0]['pwd'])
                    db.session.add(user)
                    try:
                        db.session.commit()
                       # return "user_success"
                    except:
                        db.session.rollback()
                       # return {},400
                    

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
                    #return "user_deleted"

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
                    #return "user_updated"
        except IntegrityError:
            return {},400
            

        #return "failure",400


        time.sleep(body.count(b'.'))
        print(" [x] Done")
        ch.basic_ack(delivery_tag=method.delivery_tag)



    # channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='writeq', on_message_callback=callback)

    channel.start_consuming()
    
elif(os.environ["WT"]=="slave"):
    connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rmq'))
    channel = connection.channel()

    channel.exchange_declare(exchange='logs', exchange_type='fanout')
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue

    channel.queue_bind(exchange='logs', queue=queue_name)

    print(' [*] Waiting for messages. To exit press CTRL+C')



    def callback(ch, method, properties, body):

        #data=[]
        print(" [x] Received %r" % body)

        data= (json.loads(body.decode('utf-8').replace("'", '"')))

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
        except IntegrityError:
            return {},400
            

        return "failure",400


        time.sleep(body.count(b'.'))
        print(" [x] Done")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
        
    channel.queue_declare(queue='rpc_queue',durable=True)

    def callback1(body):
        data=[]
        print(" [x] Received %r" % body)

        data.append(json.loads(body.decode('utf8').replace("'", '"')))
        
        col = data[0]["column"]
        val = data[0]["value"]
        catalog = {}
        catalog['output'] = []

        if data[0]['table'] == "user":
            if col == "email":
                information = table_user.query.filter_by(email=val).all()
            if col == "pwd":
                information = table_user.query.filter_by(pwd=val).all()
            for i in information:
                op = {}
                op['email'] = i.email
                op['pwd'] = i.pwd
                catalog['output'].append(op)
            print('length',len(catalog['output']))
            if len(catalog['output']):
                return catalog['output']
            else:
                return {},204

                  
       


    def on_request(ch, method, props, body):

        print(" [.] reading(%s)" )
        response = callback1(body)

        ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = props.correlation_id),body=str(response))
        print('testing')
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='rpc_queue', on_message_callback=on_request)
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

    print(" [x] Awaiting RPC requests")

    channel.start_consuming()




if __name__ == '__main__':  
    app.debug=True
    app.run(host='0.0.0.0')

