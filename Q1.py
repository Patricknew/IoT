import urllib2,contextlib
from datetime import datetime
from collections import OrderedDict
import sys
from pytz import timezone
import gtfs_realtime_pb2
import google.protobuf
import collections
from decimal import Decimal

import vehicle,alert,tripupdate

def ini():
    m = {}
    m['routeId'] = 'N/A'
    m['startDate'] = 'N/A'
    m['direction'] = 'N/A'
    m['currentStopId'] = 'N/A'
    m['currentStopStatus'] = 'N/A'
    m['vehicleTimeStamp'] = 'N/A'
    m['futureStopData'] = 'N/A'
    m['timeStamp'] = None
    return m

class mtaUpdates(object):

    # Do not change Timezone
    TIMEZONE = timezone('America/New_York')

    # feed url depends on the routes to which you want updates
    # here we are using feed 1 , which has lines 1,2,3,4,5,6,S
    # While initializing we can read the API Key and add it to the url

    feedurl = 'http://datamine.mta.info/mta_esi.php?feed_id=1&key='

    VCS = {1:"INCOMING_AT", 2:"STOPPED_AT", 3:"IN_TRANSIT_TO"}
    tripUpdates = []
    alerts = []

    def __init__(self,apikey):
        self.feedurl = self.feedurl + apikey

    # Method to get trip updates from mta real time feed
    def getTripUpdates(self):
        feed = gtfs_realtime_pb2.FeedMessage()
        try:
            with contextlib.closing(urllib2.urlopen(self.feedurl)) as response:
                d = feed.ParseFromString(response.read())
        except (urllib2.URLError, google.protobuf.message.DecodeError) as e:
            print "Error while connecting to mta server " +str(e)

        timestamp = feed.header.timestamp
	#print type(timestamp)
        nytime = datetime.fromtimestamp(timestamp,self.TIMEZONE)

        trips={}

        for entity in feed.entity:
            # print entity
            # Trip update represents a change in timetable
            if entity.trip_update and entity.trip_update.trip.trip_id:
                update = tripupdate.tripupdate()
                ##### INSERT TRIPUPDATE CODE HERE ####
                update.tripId = str(entity.trip_update.trip.trip_id)
		#print str(update.tripId)[-4]
                update.routeId = entity.trip_update.trip.route_id
                update.startDate = entity.trip_update.trip.start_date
		st = update.tripId
                if st.split('.')[1] == '':
			update.direction = st.split('.')[2][0]
                else:
         		update.direction = st.split('.')[1][0]
		#print update.direction
		#print entity.trip_update.trip.direction_id
                if entity.trip_update.vehicle:
                      update.vehicleData = entity.trip_update.vehicle
                tmp = collections.OrderedDict()
                len_stop = len(entity.trip_update.stop_time_update)
                for i in range(len_stop):
            	    #print entity.trip_update.stop_time_update[i].stop_id
		    #print entity.trip_update.stop_time_update[i].arrival.time
 		    #print entity.trip_update.stop_time_update[i].departure.time
                    tmp[str(entity.trip_update.stop_time_update[i].stop_id)] = [
                        entity.trip_update.stop_time_update[i].arrival.time,
                        entity.trip_update.stop_time_update[i].departure.time
                    ]
		#print tmp.items()
		#tmp = collections.OrderedDict()
                update.futureStops = str(tmp.items())
		#print update.futureStops

                if update.tripId not in trips.keys():
                    trips[update.tripId] = ini()
                trips[update.tripId]['routeId'] = update.routeId
                trips[update.tripId]['startDate'] = update.startDate
                trips[update.tripId]['direction'] = update.direction
                trips[update.tripId]['futureStopData'] = update.futureStops
		trips[update.tripId]['timeStamp'] =  timestamp
                
		#print 'tripId: ',update.tripId, 
	        #print 'routeId: ',update.routeId, 
                #print 'startDate: ',update.startDate

            if entity.vehicle and entity.vehicle.trip.trip_id:
                v = vehicle.vehicle()
                ##### INSERT VEHICLE CODE HERE #####
                v.currentStopNumber = entity.vehicle.current_stop_sequence
                v.currentStopId = entity.vehicle.stop_id
                v.timestamp = entity.vehicle.timestamp
                v.currentStopStatus = self.VCS[int(entity.vehicle.current_status)+1]
                #print v.currentStopNumber, v.currentStopId, 
		#print v.currentStopStatus
                if entity.vehicle.trip.trip_id not in trips.keys():
                    trips[entity.vehicle.trip.trip_id] = ini()
                trips[entity.vehicle.trip.trip_id]['currentStopId'] = v.currentStopNumber
                trips[entity.vehicle.trip.trip_id]['currentStopStatus'] = v.currentStopStatus
                trips[entity.vehicle.trip.trip_id]['vehicleTimeStamp'] = v.timestamp
                trips[entity.vehicle.trip.trip_id]['timeStamp']= timestamp
		
            if entity.alert:
                a = alert.alert()
                #### INSERT ALERT CODE HERE #####
                a.alertMessage = []
                mes_len = len(entity.alert.header_text.translation)
                for i in range(mes_len):
                    a.alertMessage.append(entity.alert.header_text.translation[i].text)
                a.tripId = []
		trip_len = len(entity.alert.informed_entity)
                for i in range(trip_len):
                     a.tripId.append(entity.alert.informed_entity[i].trip.trip_id)
                     a.routeId[entity.alert.informed_entity[i].trip.trip_id] = entity.alert.informed_entity[i].trip.route_id
                     if entity.alert.informed_entity[i].trip.start_date:
		                  a.startDate[entity.alert.informed_entity[i].trip.trip_id] = entity.alert.informed_entity[i].trip.start_date
                self.alerts.append(a)
		
        #print trips
        self.tripUpdates.append(trips) 
	return self.tripUpdates
        
    # END OF getTripUpdates method
'''
with open('/home/root/Lab3/key.txt','rb') as keyfile:
        APIKEY = keyfile.read().rstrip('\n')
        keyfile.close()

m = mtaUpdates(APIKEY)
m.getTripUpdates()
'''
