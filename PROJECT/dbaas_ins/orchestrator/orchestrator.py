#!/usr/bin/env python
import csv
import re 
import json
import datetime
import requests
from flask import Flask, render_template,jsonify,request,abort
import pika
import sys
from kazoo.client import KazooClient
from kazoo.client import KazooState
#from kazoo.handlers.gevent import SequentialGeventHandler
import time
import os
from kazoo.exceptions import ConnectionLossException
from kazoo.exceptions import NoAuthException
import docker
#-----------------------Zookeeper------------------------------------------
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

#zk.delete("/election", recursive = True)
if not(zk.exists("/election")):
    zk.create("/election", b"0")
    print("Created a main node for election --- should have data as pid of leader")

@zk.ChildrenWatch("/election")
def watch_children(children):
    ##### listing all the running containers

    #client = docker.DockerClient(base_url='tcp://127.0.0.1')
    client = docker.from_env()
    print("####################### Printing all the running containers ######################")
    for container in client.containers.list():
        print(container.name)
    print("####################### Printing all the running containers ######################")

    print("\n\n-------------------WATCHING /election -------------------\n")
    print("Children are : %s" % children)
    pids = []
    for child in children:
        print("Current child:", child)
        curr_path = '/election/' + child
        curr_data, stat = zk.get(curr_path)
        curr_pid = int(curr_data.decode('utf-8'))
        print("\tCurr pid:", curr_pid)
        pids.append(curr_pid)

    old_noc = int(os.environ["NOC"])
    new_noc = len(pids)
    os.environ["NOC"] = str(new_noc)
    print("The old number of children = ", old_noc)
    print("The new number of children = ", new_noc)

    if (len(pids) != 0):
        min_pid = min(pids)
        print("The smallest pid:", min_pid)
        new_master = bytes(str(min_pid), 'utf-8')
        old_master = zk.get("/election")
        old_master = int(curr_data.decode('utf-8'))
        print("Old master = ", old_master, "New master = ", min_pid)

        if (old_master == 0):
            zk.set("/election", new_master)
            print("Setting the master for the first time")

        elif (old_master == min_pid):
            print("A slave either died or was created")
            if (old_noc > new_noc):
                print("A slave died")
                client.containers.run("mycc_slave", "python3 -u slave.py", detach = True, environment = {"WT":"slave"}, links = {'zoo':'zoo', 'rmq':'rmq'}, network = "mycc_default")
                '''
                new_noc = int(os.environ["NOC"]) + 1
                os.environ["NOC"] = str(new_noc)
                print("modified noc = ", os.environ["NOC"])
                '''
                print("Done creating new slave")

        else:
            print("The master died")
            client.containers.run("mycc_master", "python3 -u master.py", detach = True, environment = {"WT":"master"}, links = {'zoo':'zoo', 'rmq':'rmq'}, network = "mycc_default")
            '''
            new_noc = int(os.environ["NOC"]) + 1
            os.environ["NOC"] = str(new_noc)
            print("modified noc = ", os.environ["NOC"])
            '''

            print("Done creating new master")
            zk.set("/election", new_master)

    print("\n--------------- DONE WATCHING /election ---------------------\n")

#-----------------------------------------------------------------

app = Flask(__name__)

import uuid
connection = pika.BlockingConnection(pika.ConnectionParameters(host='rmq'))
channel = connection.channel()
if(channel):
    print("created")
channel.queue_declare(queue='writeq', durable=True)


@app.route("/api/v1/worker/list",methods=["GET"])
def workers_list(): 
    client = docker.from_env()
    list_of_pids = []
    for container in client.containers.list():
        if "master" in container.name or "slave" in container.name:
            list_of_pids.append(container.id)
    return json.dumps(sorted(list_of_pids)),200

@app.route("/api/v1/crash/master",methods=["POST"])
def master_crash():
    client = docker.from_env()
    for container in client.containers.list():
        var = container.attrs['Config']['Env'][0]
        res = var.split(',')[0].split('=')[1]
        print('-----------------',res)
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
        print('-----------------',res)
        if "slave" in res:
            list_of_pids.append(container.id)
    sorted_list = sorted(list_of_pids)
    id_delete = sorted_list[-1]
    for container in client.containers.list():
        if container.id == id_delete:
            container.stop()
            client.containers.prune()
            return jsonify(container.id),200
    return {},400



@app.route("/api/v1/db/write",methods=["POST"])
def writeapi():
    message = request.get_json()
    print(message)
    channel.basic_publish(
        exchange='',
        routing_key='writeq',
        body=json.dumps(message),
        properties=pika.BasicProperties(
            delivery_mode=2,  # make message persistent
        ))
    print(" [x] Sent %r" % message)
    # connection.close()
    return {},200

@app.route("/api/v1/db/read",methods=["GET"])
def readapi():
    message = request.get_json()
    print(message)
    print(" [x] Sent %r" % message)
    # connection.close()
    #return {},200
    class Read_api(object):

        def __init__(self):
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host='rmq'))

            self.channel = self.connection.channel()

            result = self.channel.queue_declare(queue='', exclusive=True)
            self.responseq = result.method.queue

            self.channel.basic_consume(
                queue=self.responseq,
                on_message_callback=self.on_response,
                auto_ack=True)

        def on_response(self, ch, method, props, body):
            print("in")
            if self.corr_id == props.correlation_id:
                self.response = body

        def call(self, n):
            self.response = None
            print("----here----",n)
            self.corr_id = str(uuid.uuid4())
            self.channel.basic_publish(
                exchange='',
                routing_key='rpc_queue',
                properties=pika.BasicProperties(
                    reply_to=self.responseq,
                    correlation_id=self.corr_id,
                ),
                body=str(n))
            while self.response is None:
                self.connection.process_data_events()
            return (self.response)


    read_rpc = Read_api()

    print(" [x] printing read elements")
    response = read_rpc.call(message)
    print(" [.] Got %r" % response)

    # connection.close()
    #print("+++++++",response.decode('utf-8'))
    a=[]
    a.append(json.loads(response.decode('utf-8').replace("'", '"')))
    print("**********",a)
    if(a[0][0]=="400"):
        return {},400
    elif(a[0][0]=="200"):
        return {},200
    else:
        return jsonify(json.loads(response.decode('utf-8').replace("'", '"'))),200

if __name__ == "__main__":  
    app.debug=True
    app.run(host='0.0.0.0',port=5000)
