# *********************************************************************************************
# Program to update dynamodb with latest data from mta feed. It also cleans up stale entried from db
# Usage python dynamodata.py
# *********************************************************************************************
import json,time,sys
from collections import OrderedDict
import threading
import time

import boto3
from boto3.dynamodb.conditions import Key,Attr
from decimal import Decimal 

sys.path.append('../utils')
import tripupdate,vehicle,alert,mtaUpdates,aws

### YOUR CODE HERE ####
dynamodb = aws.getResource('dynamodb','us-east-1')
#endpoint_url="https://dynamodb.us-east-1.amazonaws.com")
#dynamodb = aws.getResource('dynamodb','us-east-1')

# Create the DynamoDB table.
try:
    mta = dynamodb.create_table(
        TableName='mta',
        KeySchema=[
            {
                'AttributeName': 'tripId',
                'KeyType': 'HASH'
	    },

            {
                'AttributeName': 'routeId',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
           {   'AttributeName':'tripId',
               'AttributeType':'S'
           },
           {
                'AttributeName': 'routeId',
                'AttributeType': 'S'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 15,
            'WriteCapacityUnits': 15
        }
    )
    mta.meta.client.get_waiter('table_exists').wait(TableName='mta')
except:
    print "Table already exists!!!"
    mta = dynamodb.Table('mta')


with open('/home/ec2-user/Lab3/key.txt','rb') as keyfile:
        APIKEY = keyfile.read().rstrip('\n')
        keyfile.close()

mutex = threading.Lock()

def add():
    try:
        while True:
	    print 'Start to add!'
            #global APIKEY
            #time.sleep(30)

            t = {}
            t['routeId'] = 'N/A'
            t['startDate'] = 'N/A'
            t['direction'] = 'N/A'
            t['futureStopData'] = 'N/A'
            t['currentStopId'] = 'N/A'
            t['currentStopStatus'] = 'N/A'
            t['vehicleTimeStamp'] = 'N/A'
            t['timeStamp'] = time.time()

            if mutex.acquire():
                m = mtaUpdates.mtaUpdates(APIKEY)
                tmp = m.getTripUpdates()
		trips = tmp[0]
		#print trips
		global mta
                #with mta.batch_writer() as batch:
                for key in trips:
			#print key
                        for k in trips[key]:
                            if k == 'routeId':
                                t['routeId'] = trips[key]['routeId']
                            if k == 'startDate':
                                t['startDate'] = trips[key]['startDate']
                            if k == 'direction':
                                t['direction'] = trips[key]['direction']
                            if k == 'futureStopData':
                                t['futureStopData'] = trips[key]['futureStopData']
                            if k == 'currentStopId':
                                t['currentStopId'] = trips[key]['currentStopId']
                            if k == 'currentStopStatus':
                                t['currentStopStatus'] = trips[key]['currentStopStatus']
                            if k == 'vehicleTimeStamp':
                                t['vehicleTimeStamp'] = trips[key]['vehicleTimeStamp']
                            if k == 'timeStamp':
                                t['timeStamp'] = trips[key]['timeStamp']
                        #with mta.batch_writer() as batch:
                        mta.put_item(
                                Item={
                                    'tripId': str(key),
                                    'routeId': str(t['routeId']),
                                    'startDate': t['startDate'],
                                    'direction': t['direction'],
                                    'futureStopData': t['futureStopData'],
                                    'currentStopId': t['currentStopId'],
                                    'vehicleTimeStamp': t['vehicleTimeStamp'],
                                    'currentStopStatus': t['currentStopStatus'],
                                    'timeStamp': Decimal(t['timeStamp'])
                                    })
	        print " Add Done!"
                mutex.release()
	    time.sleep(30)
    except KeyboardInterrupt:
        print 'Stop'
    except:
        print 'Wrong add'

def clean():
    try:
        while True:
            print 'Start to clean!!'
            time.sleep(60)
            if mutex.acquire():
                t = Decimal(time.time() - 120)
                response = mta.scan(
                    FilterExpression=Attr('timeStamp').lt(t)
                )
                items = response['Items']
                for item in items:
			hkey = item['tripId']
                        rkey = item['routeId']
			mta.delete_item(
                             Key = {
				'tripId':hkey,
          			'routeId':rkey,
			     }
                        )
		print 'Delete done!'
                mutex.release()
		time.sleep(60)
    except KeyboardInterrupt:
        print 'Stop '
    except:
        print 'Wrong Clean!!!'

try:
     flag=True
     while True:
	if flag==True:
	  c = threading.Thread(target=add)
	  c.setDaemon(True)
 	  c.start()

	  cc = threading.Thread(target=clean)
	  cc.setDaemon(True)
	  cc.start()
	  flag=False

except KeyboardInterrupt:
	exit
	print 'Main exit!!!'

