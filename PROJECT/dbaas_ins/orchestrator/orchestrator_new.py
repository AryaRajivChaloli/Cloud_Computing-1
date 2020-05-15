#!/usr/bin/env python

#importing the necessary libraries
import csv
import re 
import json
import datetime
from datetime import timedelta
import requests
from flask import Flask, render_template,jsonify,request,abort
import pika
import threading
import sys
from kazoo.client import KazooClient
from kazoo.client import KazooState
import time
import os
from kazoo.exceptions import ConnectionLossException
from kazoo.exceptions import NoAuthException
import docker
from random import random
from threading import Timer, Event


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

# Creating the main node which acts as a parent
# Data of this node is the PID of the master
# Initially data is 0 as no children have been created thus no master is present
if not(zk.exists("/election")):
    zk.create("/election", b"0")

# Setting up a watch on all the children of the main node
@zk.ChildrenWatch("/election")
def watch_children(children):
    client = docker.from_env()
    
    for container in client.containers.list():
        if container.name == "master":
            cont = container

    # Iterate through all the children and store their PIDs in a list
    pids = []
    for child in children:
        curr_path = '/election/' + child
        curr_data, stat = zk.get(curr_path)
        curr_pid = (curr_data.decode('utf-8'))
        pids.append(curr_pid)
        
    #NOC = Number of children, acts as an env variable to keep track of whether a new slave was created or killed
    import os
    old_noc = int(os.environ["NOC"])
    new_noc = len(pids)
    os.environ["NOC"] = str(new_noc)

    if (len(pids) != 0):

        # The minimum PID out of all the children has to be the master
        min_pid = min(pids)
        new_master = bytes(str(min_pid), 'utf-8')
        
        # Getting the PID of the current master
        old_master, stat = zk.get("/election")
        old_master = (old_master.decode('utf-8'))

        # If the current master's PID = 0, then we are setting up the master for the first time
        # as initially the data was set to 0
        if (old_master == 0):
            zk.set("/election", new_master)

        # If the current master is the same as the minimum PID then the master hasn't changed
        # Either a slave has been created or was killed
        elif (old_master == min_pid):
            
            # A slave was killed
            if (old_noc > new_noc):
            
                # Generate a name for the slave
                name = "slave"+str(random())
                
                # Start a new slave container
                client.containers.run("mycc_master","python3 -u /slave/slave_new.py",network="mycc_default",environment = {"WT":"slave","NOC":"0"},name=name,links = {'zoo':'zoo', 'rmq':'rmq'},volumes={'/home/ubuntu/CC_instance/Mycc/slave/slave_new.py':{'bind':'/slave/slave_new.py'},'/home/ubuntu/CC_instance/Mycc/master/master.db':{'bind':'/master.db'},'/var/run/docker.sock':{'bind':'/var/run/docker.sock'},'/usr/local/bin/docker':{'bind':'/usr/local/bin/docker'}},detach=True)

        # The master was killed
        # Start a new slave container and change the data of the main node to the minimum PID found
        else:
        
            name = "slave"+str(random())
            client.containers.run("mycc_slave","sh -c sleep 20 && python3 -u /slave/slave_new.py",network="mycc_default",environment = {"WT":"slave","NOC":"0"},name=name,links = {'zoo':'zoo', 'rmq':'rmq'},volumes={'/home/ubuntu/CC_instance/Mycc/slave/slave_new.py':{'bind':'/slave/slave_new.py'},'/home/ubuntu/CC_instance/Mycc/master/master.db':{'bind':'/master.db'},'/var/run/docker.sock':{'bind':'/var/run/docker.sock'},'/usr/local/bin/docker':{'bind':'/usr/local/bin/docker'}},detach=True)
            # Setting the new data
            zk.set("/election", new_master)

# Starting the flask app
app = Flask(__name__)

import uuid
global timer

try:
    # Establish connection with RabbitMQ server, hostname is the name of the container running RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rmq',heartbeat=1600))
    channel = connection.channel()
except:
    exit(1)

# Declare the required queues
channel.queue_declare(queue='writeq', durable=True)
channel.queue_declare(queue='copyq', durable=True)


count = 0

# Function to return the count of the number of read requests received
def read_count():
    global count
    return count

def reset_count():
    global count
    count = 0
    return count

def increment_count():
    global count
    count += 1
    return count

# Function to create a new slave 
def slave_create():
    client = docker.from_env()
    name = "slave"+str(random())  
    client.containers.run("mycc_master","python3 -u /slave/slave_new.py",network="mycc_default",environment = {"WT":"slave","NOC":"0"},name=name,links = {'zoo':'zoo', 'rmq':'rmq'},volumes={'/home/ubuntu/CC_instance/Mycc/slave/slave_new.py':{'bind':'/slave/slave_new.py'},'/home/ubuntu/CC_instance/Mycc/master/master.db':{'bind':'/master.db'},'/var/run/docker.sock':{'bind':'/var/run/docker.sock'},'/usr/local/bin/docker':{'bind':'/usr/local/bin/docker'}},detach=True)

# Function to get the number of slaves currently running
def get_slave_count():
    client = docker.from_env()
    slave_cnt = 0
    for container in client.containers.list():
        if "slave" in container.name:
            slave_cnt += 1

    return slave_cnt

# To programmatically add 'n' slaves
def add_slaves(n):
    for i in range(n):
        slave_create()

# To programmatically kill a slave
def slave_crash():
    client = docker.from_env()
    for container in client.containers.list():
        if container.name!='master' and container.name!='orchestrator' and container.name!='zoo' and container.name!='rmq' and container.name!='slave':
            container.stop()
            client.containers.prune()
            os.environ["NOC"] = int(os.environ["NOC"]) - 1
            return 1

# Function to remove 'n' slaves
def remove_slaves(n):
    for i in range(n):
        crash_url = 'http://52.87.52.143:80/api/v1/crash/slave'
        r2 = requests.post(crash_url)
        os.environ["NOC"] = str(int(os.environ["NOC"]) - 1)


# Function to implement scaling
def scaler_function():
    global timer
    
    # Get the number of read requests received
    req_cnt = read_count()
    
    # Get required number of running slaves, based on how many requests were received
    i = ((req_cnt-1)//20)+1 if req_cnt else 1
    
    # Get current number of slaves
    slave_cnt = get_slave_count()
    
    # If current number of slaves are less than required, start more slaves, else kill slaves
    if slave_cnt < i:
        add_slaves(i-slave_cnt)
        slave_cnt = get_slave_count()

    if slave_cnt > i:
        remove_slaves(slave_cnt-i)
    reset_count()


@app.route("/api/v1/worker/list",methods=["GET"])
def workers_list(): 
    client = docker.from_env()
    list_of_pids = []
    for container in client.containers.list():
        var = container.attrs['Config']['Env'][0]
        res = var.split(',')[0].split('=')[1]

        if "master" in res or "slave" in res:
            cid = container.attrs['State']['Pid']
            list_of_pids.append((cid,container.name))
    return json.dumps(sorted(list_of_pids)),200


@app.route("/api/v1/crash/master",methods=["POST"])
def master_crash():
    client = docker.from_env()
    for container in client.containers.list():
        # Get the name of the container to check if it is a master
        var = container.attrs['Config']['Env'][0]
        res = var.split(',')[0].split('=')[1]

        # Stop if it is a master
        if "master" in res:
            container.stop()
            client.containers.prune()
            
    return {},200


@app.route("/api/v1/crash/slave",methods=["POST"])
def slave_crash():
    client = docker.from_env()
    list_of_pids = []
    
    for container in client.containers.list():
        var = container.attrs['Config']['Env'][0]
        res = var.split(',')[0].split('=')[1]

        # Get a list of all the slave containers
        if "slave" in res:
            cid = container.attrs['State']['Pid']
            list_of_pids.append(cid)

    # Get the slave with the highes PID, kill it
    sorted_list = sorted(list_of_pids)
    id_delete = sorted_list[-1]
    for container in client.containers.list():
        var = container.attrs['Config']['Env'][0]
        res = var.split(',')[0].split('=')[1]
        cid = container.attrs['State']['Pid']
        if cid == id_delete:
            container.stop()
            client.containers.prune()
            return jsonify(cid),200
    return {},400



@app.route("/api/v1/db/write",methods=["POST"])
def writeapi():
    # Get the data received in the body of the request
    message = request.get_json()

    # Using the default exchange to send the data to the write queue
    channel.basic_publish(
        exchange='',
        routing_key='writeq',
        body=json.dumps(message),
    )

    # Sending the data to the copy queue
    channel.basic_publish(
        exchange='',
        routing_key='copyq',
        body=json.dumps(message),
        # make message persistent
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))

    return {},200

@app.route("/api/v1/db/read",methods=["GET"])
def readapi():
    global has_requests_started
    global timer
    
    # Increment the read requests counter
    increment_count()

    # Start a timer once the first request is received
    if not has_requests_started:
        has_requests_started = True
        # Call the scaler function after 180 sec
        timer = threading.Timer(3*60, scaler_function).start()

    # Get the body of the request
    message = request.get_json()

    class Read_api(object):

        def __init__(self):
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host='rmq',heartbeat=1600))

            self.channel = self.connection.channel()

            result = self.channel.queue_declare(queue='', exclusive=True)
            self.responseq = result.method.queue

            self.channel.basic_consume(
                queue=self.responseq,
                on_message_callback=self.on_response,
                auto_ack=True)

        def on_response(self, ch, method, props, body):
            if self.corr_id == props.correlation_id:
                self.response = body

        def call(self, n):
            self.response = None
            self.corr_id = str(uuid.uuid4())
            self.channel.basic_publish(
                exchange='',
                routing_key='rpc_queue',
                properties=pika.BasicProperties(
                    reply_to=self.responseq,
                    correlation_id=self.corr_id,
                ),body=str(n))

            while self.response is None:
                self.connection.process_data_events()
            return (self.response)


    read_rpc = Read_api()

    response = read_rpc.call(message)
    response = response.decode('utf-8')
    r1 = response.split(',')[0]

    if('{}' not in r1):
        a=[]
        a.append(json.loads(response.replace("'", '"')))
        return jsonify(json.loads(response.replace("'", '"'))),200
    else:
        return {},204

@app.route('/api/v1/db/clear', methods = ['POST'])
def cleardb():
    # Post the clear db requests as a write request with an operation = clear (will handle clearing of the db)
    response2 = requests.post("http://52.87.52.143:80/api/v1/db/write",json={'operation':'clear'})
    if(response2.status_code == 200):
        return {},200
    else:
        return {},response2.status_code


if __name__ == "__main__":  
    global has_requests_started
    has_requests_started = False
    app.debug=True
    app.run(host='0.0.0.0',port=80,use_reloader=False)
