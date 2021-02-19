
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 19 10:15:03 2020
@author: Lisa
"""
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct  7 11:47:24 2020
@author: Lisa
"""

from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict
import requests
import json
import pandas as pd
import numpy as np
from collections import OrderedDict
import datetime
from datetime import date
import urllib
from urllib.error import HTTPError
import time
import io
import threading
import zipfile
import multiprocessing
from multiprocessing.pool import ThreadPool as Pool
from datetime import timedelta
import os
import ssl


def extract_GTFS_static(url):
    """
    Download a ZIP file and extract its contents 
    Used to get list of all routes
    """
    response = requests.get(url)
    with zipfile.ZipFile(io.BytesIO(response.content)) as thezip:
        df = pd.read_csv(thezip.open('routes.txt'))
        return df

# Region = New York 
def pull_mta_bus():
    ssl._create_default_https_context = ssl._create_unverified_context
    busfeed = gtfs_realtime_pb2.FeedMessage()
    tripfeed = gtfs_realtime_pb2.FeedMessage()
    api_key = 'ab069f16-0419-41d9-903f-2020458dbc42'
    
    # requests will fetch the results from the url
    busresponse = requests.get('http://gtfsrt.prod.obanyc.com/vehiclePositions?key={api_key}', allow_redirects=True)
    busfeed.ParseFromString(busresponse.content)

    #request for trip feed
    tripresponse = requests.get('http://gtfsrt.prod.obanyc.com/tripUpdates?key={api_key}', allow_redirects=True)
    tripfeed.ParseFromString(tripresponse.content)
    
    # Check if request is working
    #print('There are {} buses in the dataset.'.format(len(busfeed.entity)))
    #print('There are {} trip updates in the dataset.'.format(len(tripfeed.entity)))
    
    if len(tripfeed.entity) == 0:
           return
    
    dict_obj_bus = MessageToDict(busfeed)
    dict_obj_trip = MessageToDict(tripfeed)
    
    # Create dataframe of all vehicles that are currently on the network    
    collector = []
    for bus in dict_obj_bus['entity']:
        row = OrderedDict()
        row['id'] = bus['id']
        row['route_id'] = bus['vehicle'].get('trip', {}).get('routeId')
        row['trip_id'] = bus['vehicle'].get('trip', {}).get('tripId')
        row['latitude'] = bus['vehicle']['position'].get('latitude','')
        row['longitude'] = bus['vehicle']['position'].get('longitude','')
        row['bearing'] = bus['vehicle']['position'].get('bearing','')
        row['timestamp'] = bus['vehicle'].get('timestamp','')
        row['bus_stop_id'] = bus['vehicle'].get('stopId','')
        row['vehicle_id'] = bus['vehicle']['vehicle'].get('id','')
        row['label'] = bus['vehicle']['vehicle'].get('label','')
        collector.append(row)
        
    df_bus = pd.DataFrame(collector)
    df_bus['humantime'] = df_bus.apply( lambda row: datetime.datetime.fromtimestamp(int(row['timestamp'])),axis=1 )
    feedtime = int(dict_obj_bus['header']['timestamp']) 
    
    # Create dataframe of all trips updates
    collector = []
    for trip in dict_obj_trip['entity']:
        if trip['tripUpdate'].get('stopTimeUpdate') is not None:
            for i in range(len(trip['tripUpdate']['stopTimeUpdate'])):
                row = OrderedDict()
                row['trip_id'] = trip['tripUpdate'].get('trip', {}).get('tripId')
                row['id'] = trip.get('id')
                row['vehicle_id'] = trip['tripUpdate'].get('vehicle', {}).get('id')
                row['trip_startday'] = trip['tripUpdate'].get('trip', {}).get('startDate')
                row['route_id'] = trip['tripUpdate'].get('trip', {}).get('routeId')
                row['delay'] = trip['tripUpdate'].get('delay')
                row['arrival_time'] = trip['tripUpdate']['stopTimeUpdate'][i].get('arrival', {}).get('time')  
                row['departure_time'] = trip['tripUpdate']['stopTimeUpdate'][i].get('departure', {}).get('time')  
                row['trip_stop_id'] = trip['tripUpdate']['stopTimeUpdate'][i].get('stopId')
                row['trip_stop_seq'] = trip['tripUpdate']['stopTimeUpdate'][i].get('stopSequence')  
                collector.append(row) 
                
    df_trip = pd.DataFrame(collector)
    # Merge the two feeds together to get the delay times of each bus
    merged_rt = df_bus.merge(df_trip, on=['trip_id', 'vehicle_id'], how='left')
    merged_rt = merged_rt.drop_duplicates(subset=['trip_id', 'vehicle_id'])
    # Clean the format of the dataframe
    merged_rt = merged_rt.drop(columns=['id_y', 'route_id_y'])
    merged_rt = merged_rt.rename(columns={"id_x": "id", "route_id_x": "route_id"})
    merged_rt['mode'] = "Bus"
    merged_rt['agency'] = "MTA Bus"
    merged_rt['region'] = 'New York'
    return merged_rt



def pull_mta_mnr():
    ssl._create_default_https_context = ssl._create_unverified_context
    # Parse Realtime Subway Feed
    api_key = 'CK3pt1u1k71RlZ7OzDnUm43Qa74inIjp7NkFOAxp'
    headers = {
        "x-api-key": api_key
    }
        
    feed = gtfs_realtime_pb2.FeedMessage()
    url = 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/mnr%2Fgtfs-mnr'   # to make as a function to pass through later
    response = urllib.request.Request(url, headers=headers)
    xml = urllib.request.urlopen(response)
    feed.ParseFromString(xml.read())
    
    dict_obj_feed = MessageToDict(feed)
    
    if len(feed.entity) == 0:
           return
    
    collector = []
    for message in dict_obj_feed['entity']:
        # Only get trains that are currently active in the system
        if message['vehicle'].get('position') is not None or message['tripUpdate']['stopTimeUpdate'][0].get('arrival', {}).get('delay') is not None:
            row = OrderedDict()
            row['trip_id'] = message['id']
            row['route_id'] = message['tripUpdate']['trip']['routeId']
            row['start_date'] = message['tripUpdate']['trip']['startDate']
            row['start_time'] = message['tripUpdate']['trip']['startTime']
            row['route_id'] = message['tripUpdate']['trip']['routeId']  
            row['timestamp'] = message['vehicle'].get('timestamp')
            row['stop_id'] = message['vehicle'].get('stopId')
            for i in range(len(message['tripUpdate']['stopTimeUpdate'])):
                if message['tripUpdate']['stopTimeUpdate'][i]['stopId'] == message['vehicle'].get('stopId'):
                    row['delay'] = message['tripUpdate']['stopTimeUpdate'][0].get('arrival', {}).get('delay')
                    row['arrival_time'] = message['tripUpdate']['stopTimeUpdate'][i].get('arrival', {}).get('time') 
                    row['departure_time'] = message['tripUpdate']['stopTimeUpdate'][i].get('departure', {}).get('time')
            row['current_status'] = message['vehicle'].get('currentStatus')
            collector.append(row)
            
    df = pd.DataFrame(collector)
    df['humantime'] = df.apply( lambda row: datetime.datetime.fromtimestamp(int(row['timestamp'])),axis=1 )
    feedtime = int(dict_obj_feed['header']['timestamp'])
    df = df.rename(columns = {"trip_short_name": "trip_id"})

    # If there is an arrival/departure time available and the delay field is 'Nan', it should be replaced with 0
    for i, row in df.iterrows():
        if pd.isnull(row['arrival_time']):
            pass
        elif pd.isnull(row['delay']):
            df.at[i, 'delay'] = 0
    df['mode'] = "Rail"
    df['agency'] = 'MTA Metro North'
    df['region'] = 'New York'
    return df



def pull_mta_lirr():
    ssl._create_default_https_context = ssl._create_unverified_context
    # Parse the realtime subway feed
    api_key = 'CK3pt1u1k71RlZ7OzDnUm43Qa74inIjp7NkFOAxp'
    headers = {
        "x-api-key": api_key
    }
        
    feed = gtfs_realtime_pb2.FeedMessage()
    url = ' https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/lirr%2Fgtfs-lirr'   # to make as a function to pass through later
    response = urllib.request.Request(url, headers=headers)
    xml = urllib.request.urlopen(response)
    feed.ParseFromString(xml.read())
    
    if len(feed.entity) == 0:
           return
    
    dict_obj_feed = MessageToDict(feed)
    
    collector = []
    for message in dict_obj_feed['entity']:
        # Only get trains that are currently active in the system
        if message.get('vehicle') is not None:
            row = OrderedDict()
            row['trip_id'] = message['vehicle']['trip'].get('tripId')
            train_id = message['vehicle']['trip'].get('tripId')
            stop_id = message['vehicle'].get('stopId')
            row['schedule_relation'] = message['vehicle']['trip'].get('scheduleRelationship')
            row['stop_id'] = message['vehicle'].get('stopId')
            row['current_status'] = message['vehicle'].get('currentStatus')
            row['timestamp'] = dict_obj_feed['header']['timestamp']
            # loop through the trip feeds 
            for message in dict_obj_feed['entity']:
                if message.get('tripUpdate') is not None and message['tripUpdate'].get('stopTimeUpdate') is not None:
                    # Only get if the trip_ids match
                    if train_id == message['tripUpdate']['trip'].get('tripId'):
                        #print(train_id)
                        # Loop through the list until stop_id matches. Otherwise take the most recent one, which is the last one
                        for i in range(len(message['tripUpdate']['stopTimeUpdate'])):
                            if message['tripUpdate']['stopTimeUpdate'][i].get('arrival', {}).get('delay') is not None and stop_id == message['tripUpdate']['stopTimeUpdate'][i].get('stopId'):
                                row['delay'] = message['tripUpdate']['stopTimeUpdate'][i].get('arrival', {}).get('delay')
                                break
                            if message['tripUpdate']['stopTimeUpdate'][i].get('departure', {}).get('delay') is not None and stop_id == message['tripUpdate']['stopTimeUpdate'][i].get('stopId'):
                                row['delay'] = message['tripUpdate']['stopTimeUpdate'][i].get('departure', {}).get('delay')
                                break
                            else:
                                if message['tripUpdate']['stopTimeUpdate'][-1].get('arrival', {}).get('delay') is not None:
                                    row['delay'] = message['tripUpdate']['stopTimeUpdate'][-1].get('arrival', {}).get('delay')
                                elif message['tripUpdate']['stopTimeUpdate'][-1].get('departure', {}).get('delay') is not None:
                                    row['delay'] = message['tripUpdate']['stopTimeUpdate'][-1].get('departure', {}).get('delay')
                        continue
                    continue
            collector.append(row)
    df = pd.DataFrame(collector)
    df['humantime'] = df.apply( lambda row: datetime.datetime.fromtimestamp(int(row['timestamp'])),axis=1 )
    df['mode'] = "Rail"
    df['agency'] = 'Long Island Rail Road'
    df['region'] = 'New York'
    return df



def pull_septa_rail():
    ssl._create_default_https_context = ssl._create_unverified_context
    tripfeed = gtfs_realtime_pb2.FeedMessage()
    #request for trip feed
    tripresponse = requests.get('http://www3.septa.org/gtfsrt/septarail-pa-us/Trip/rtTripUpdates.pb', allow_redirects=True)
    tripfeed.ParseFromString(tripresponse.content)
    
    # Check if request is working
    #print('There are {} trip updates in the dataset.'.format(len(tripfeed.entity)))
    if len(tripfeed.entity) == 0:
           return None
    
    dict_obj_trip = MessageToDict(tripfeed)
    collector = []
    for trip in dict_obj_trip['entity']:
        if trip['tripUpdate'].get('stopTimeUpdate') is not None:
            row = OrderedDict()
            row['trip_id'] = trip['tripUpdate'].get('trip', {}).get('tripId')
            row['id'] = trip.get('id')
            row['delay'] = trip['tripUpdate']['stopTimeUpdate'][0].get('arrival', {}).get('delay')
            row['trip_stop_id'] = trip['tripUpdate']['stopTimeUpdate'][0].get('stopId')
            row['trip_stop_seq'] = trip['tripUpdate']['stopTimeUpdate'][0].get('stopSequence')
            row['timestamp'] = dict_obj_trip['header']['timestamp']
            collector.append(row)
    df = pd.DataFrame(collector)  
    df['humantime'] = df.apply( lambda row: datetime.datetime.fromtimestamp(int(row['timestamp'])),axis=1 )
    df['agency'] = 'SEPTA'
    df['mode'] = "Rail"
    df['region'] = "New York/Philadelphia"
    return df



# Region = San Francisco
def pull_sf():
    ssl._create_default_https_context = ssl._create_unverified_context
    #api_key = 'a6c3b04b-d3f0-46af-b883-307e8c5d0bae'
    api_key = '79eaf36f-796b-4e32-92b6-42fa6ef8e70e'
    url = f'http://api.511.org/transit/operators?Format=JSON&api_key={api_key}'
    r = urllib.request.urlopen(url)    
    data = json.loads(r.read())
    df = pd.DataFrame(data)
    df = df[df['Montiored'] == True].copy()
    agency_mode = df.copy()
    agency_mode = agency_mode[["Id", "PrimaryMode", "OtherModes"]]
    agency_mode['PrimaryMode'] = agency_mode['PrimaryMode'].str.capitalize()
    agency_mode = agency_mode.rename(columns={"PrimaryMode": "mode"})
    rt_agency_list = df['Id'].tolist()
    
    agencies = []
    agency_name = []
    route_id = []
    deltas = []
    for idx, agency in df.iterrows():
        status_url = f'http://api.511.org/transit/StopMonitoring?Format=JSON&api_key={api_key}&agency={agency.Id}'
        r = urllib.request.urlopen(status_url)
        data = json.loads(r.read())
        # with open("out.json", 'w') as outfile:
        #     outfile.write(json.dumps(data, indent=4))
        # print(data.keys())
        # timestamp = data['ResponseTimestamp']
        states = data['ServiceDelivery']['StopMonitoringDelivery']['MonitoredStopVisit']
        for s in states:
            vehicle = s['MonitoredVehicleJourney']
            if vehicle['MonitoredCall']['ExpectedArrivalTime'] is not None and vehicle['MonitoredCall']['AimedArrivalTime'] is not None:
                expected = datetime.datetime.strptime(vehicle['MonitoredCall']['ExpectedArrivalTime'], "%Y-%m-%dT%H:%M:%SZ")
                aimed = datetime.datetime.strptime(vehicle['MonitoredCall']['AimedArrivalTime'], "%Y-%m-%dT%H:%M:%SZ")
                tDelta = expected - aimed
                curr_agency = agency.Id
                curr_agency_name = agency.Name
                curr_route = s['MonitoredVehicleJourney']['LineRef']
                # print(vehicle['PublishedLineName'], tdelta.total_seconds()/60)
                deltas.append(tDelta.total_seconds())
                agencies.append(curr_agency)
                agency_name.append(curr_agency_name)
                route_id.append(curr_route)
                
    df_rt = pd.DataFrame(agencies)
    df_rt['delay'] = deltas   
    df_rt['Agency'] = agency_name
    df_rt['Route'] = route_id
    df_rt = df_rt.rename(columns={0: "Id"})
    df_rt = df_rt.merge(agency_mode, on=['Id'], how='left')
    
    #For agencies that contain 2 modes, we will have to specify what mode the delay is for
    #Only appliesto VTA (also known as SC)
    rail_modes = ["Orange Line", "Blue Line", "Green Line"]
    df_rt['mode'].loc[(df_rt['Route'].isin(rail_modes))] = "Rail"
    df_rt['region'] = "San Francisco"
    return df_rt



# Region = Los Angeles - none of the agencies had delay fields directly reported



# Region = Philadelphia
def pull_septa_bus():
    ssl._create_default_https_context = ssl._create_unverified_context
    response = requests.get("https://www3.septa.org/api/TransitViewAll/")
    content = json.loads(response.text)
    septa_routes = extract_GTFS_static('https://transitfeeds.com/p/septa/263/latest/download')
    route_list = septa_routes.route_id.to_list()
    collector = []
    for route in content['routes']:
        for i in route_list:
            if i in route:
                for j in range(len(route[i])):
                    row = OrderedDict()
                    row['latitude'] = route[i][j]['lat']
                    row['longitude'] = route[i][j]['lng']
                    row['vehicle_id'] = route[i][j]['VehicleID']
                    row['delay'] = route[i][j]['late']
                    row['route'] = i
                    row['trip_id'] = route[i][j]['trip']
                    collector.append(row)  
            else:
                continue
    df = pd.DataFrame(collector)
    
    # Make delay times that do not make sense as missing values
    # note that the delay times are reported in minutes for SEPTA
    df.loc[df['delay'] > 900,['delay']] = np.nan
    # Convert to seconds
    df['delay'] = df['delay'] * 60
    df['mode'] = 'Bus'
    df['agency'] = 'SEPTA'
    df['region'] =  "New York/Philadelphia"
    return df



# Region = DC
def pull_wmata_bus():
    ssl._create_default_https_context = ssl._create_unverified_context
    headers = {
        # Request headers
        'api_key': 'acf1010abac8437dbeb5f9ed3699169b',
    }
    
    tripfeed = gtfs_realtime_pb2.FeedMessage()
    url = 'https://api.wmata.com/gtfs/bus-gtfsrt-tripupdates.pb'   # to make as a function to pass through later
    response = urllib.request.Request(url, headers=headers)
    xml = urllib.request.urlopen(response)
        
    tripfeed.ParseFromString(xml.read())
    dict_obj_trip = MessageToDict(tripfeed)
    
    if not dict_obj_trip.get('entity'):
           return
    
    collector = []
    for message in dict_obj_trip['entity']:
        row = OrderedDict()
        row['trip_id'] = message['tripUpdate']['trip']['tripId']
        row['route_id'] = message['tripUpdate']['trip']['routeId']
        row['vehicle_id'] = message['tripUpdate']['vehicle']['id']
        row['delay'] = message['tripUpdate'].get('delay')
        collector.append(row)
    df_trip = pd.DataFrame(collector)
    
    busfeed = gtfs_realtime_pb2.FeedMessage()
    url = 'https://api.wmata.com/gtfs/bus-gtfsrt-vehiclepositions.pb'
    response = urllib.request.Request(url, headers=headers)
    xml = urllib.request.urlopen(response)
        
    busfeed.ParseFromString(xml.read())
    dict_obj_bus = MessageToDict(busfeed)
    
    if not dict_obj_bus.get('entity'):
           return None
    
    collector = []
    for message in dict_obj_bus['entity']:
        row = OrderedDict()
        row['vehicle_id'] = message['id']
        row['route_id'] = message['vehicle']['trip']['routeId']
        row['trip_id'] = message['vehicle']['trip']['tripId']
        collector.append(row)
    df_bus = pd.DataFrame(collector)
    
    df_merged = df_bus.merge(df_trip, on=['trip_id', 'route_id', 'vehicle_id'], how='left')
    df_merged['mode'] = "Bus"
    df_merged['agency'] = "WMATA"
    df_merged['region'] = "Washington"
    return df_merged



# Region = Chicago
def pull_cta_bus():
    """
    For CTA buses, we can only get the proportion of buses that are delayed
    but not the average delay time directly
    """
    ssl._create_default_https_context = ssl._create_unverified_context
    df_routes = extract_GTFS_static('https://www.transitchicago.com/downloads/sch_data/google_transit.zip')
    routes_list = df_routes.route_id.to_list()
    
    #Cycle through the route list and record each live bus
    collector = []
    for route in routes_list:
        response = requests.get("http://www.ctabustracker.com/bustime/api/v2/getvehicles?key=uEL6br9SmqUHTDXCmLYHBNQB7&rt=" + route + "&format=json")
        content = json.loads(response.text)
        if 'error' in content['bustime-response']:
            continue
        else:
            for i in range(len(content['bustime-response']['vehicle'])):
                row = OrderedDict()
                row['vehicle_id'] = content['bustime-response']['vehicle'][i]['vid']
                row['latitude'] = content['bustime-response']['vehicle'][i]['lat']
                row['longitude'] = content['bustime-response']['vehicle'][i]['lon']
                row['route'] = content['bustime-response']['vehicle'][i]['rt']
                row['delay'] = content['bustime-response']['vehicle'][i]['dly']
                row['trip_id'] = content['bustime-response']['vehicle'][i]['tatripid']
                collector.append(row)
    df = pd.DataFrame(collector)
    df['mode'] = "Bus"
    df['agency'] = "CTA"
    df['region'] = "Chicago"
    return df
    


def grab_rt_feeds():
    # Grab all the realtime feeds in parallel
    # Need to change this based on which agencies we have realtime feed info for
    
    # Initialize the pool
    pool = multiprocessing.Pool(8)
    #NY
    result1 = pool.apply_async(pull_mta_bus)
    result2 = pool.apply_async(pull_mta_mnr)
    result3 = pool.apply_async(pull_mta_lirr)
    #Philadelphia
    result4 = pool.apply_async(pull_septa_rail)
    result5 = pool.apply_async(pull_septa_bus)
    #Chicago
    result6 = pool.apply_async(pull_cta_bus)
    #San Francisco
    result7 = pool.apply_async(pull_sf)
    #Washington
    result8 = pool.apply_async(pull_wmata_bus)
    
    pool.close()
    pool.join()

    rt_mta_bus = result1.get()
    rt_mta_mnr = result2.get()
    rt_mta_lirr = result3.get()
    rt_septa_rail = result4.get()
    rt_septa_bus = result5.get()
    rt_cta_bus = result6.get()
    rt_sf = result7.get()
    rt_wmata_bus = result8.get()
    
    return rt_mta_bus, rt_mta_mnr, rt_mta_lirr, rt_septa_bus, rt_septa_rail, rt_cta_bus, rt_sf, rt_wmata_bus


if __name__ == "__main__":
    grab_rt_feeds()