# Ridesharing Application on AWS

The project is focussed on building a fault tolerant, highly available database as a service for the RideShare application. 

## Installation

Install docker on all the three instances namely Users , Rides and DBaas instance

```bash
$ sudo apt-get update
$ sudo apt-get install docker.io
```

## Usage for Project


To get your Rideshare application running , clone the github repo and go to the
folder PROJECT which has three folders corresponding to the names of the instances respectively. 

Make sure to be in the directory named after your instance.

Then run the following command. This command will ensure that the five containers - Zookeeper, RabbitMQ, Orchestator, Master and Slave are created.
```bash
$ sudo docker-compose up --build
```

## Usage for Assignment 1 & 2

In the folder called "ASSIGNMENT 2" there is an app.py file and a Dockerfile. The code can be run using the command 
```bash 
$ docker build --tag Rideshare:1.0 .
$ docker run --publish 8000:8080 --detach --name Rideshare Rideshare:1.0
```
The same file can be used to check for Assignment1 as well

## Usage for Assignment 3

In the folder called "ASSIGNMENT 3", there are two folders called "Rides", "Users" for the rides and users instance respectively. Inside each of the folders, there's a docker compose file which can be run using
```bash
$ docker-compose up --build
```

This will set up everything in the necessary instances

  



## Team Members
```bash
Tanvi P Karennavar
Bhavya Charan
Arya Rajiv
Aditi Manohar
```
