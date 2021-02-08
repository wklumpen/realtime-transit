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
from datetime import timezone
from datetime import timedelta
import pytz
import urllib
import time
import io
import zipfile
import rt_inputs
import multiprocessing
from multiprocessing.pool import ThreadPool as Pool
import schedule
import random
import time
import sys

sys.path.append(".")



def on_time_percent(df):
    if df is None:
        on_time_percent = np.nan
        
    else:
        on_time = df[(df['delay'] <= 300) & (df['delay'] >= -60)].shape[0]
        on_time_percent = (on_time / df['delay'].count()) * 100

    return on_time_percent



def avg_delay_abs(df):
    if df is None:
        avg_delay_abs = np.nan
        
    else:
        avg_delay_abs = df['delay'].mean()
        
    return avg_delay_abs



def avg_delay_early(df):
    if df is None:
        avg_delay_early = np.nan
        
    else:
        avg_delay_early = df.delay[df['delay'] < -60].mean()
        
    return avg_delay_early



def avg_delay_late(df):
    if df is None:
        avg_delay_late = np.nan
        
    else:
        avg_delay_late = df.delay[df['delay'] > 300].mean()
        
    return avg_delay_late



def rt_fraction(df):
    if df is None:
        fraction = np.nan
    
    else:
        fraction = (1 - df["delay"].isna().sum() / df.shape[0]) * 100
        
    return fraction



def timezone_converter(dt, tz):
    timezone = pytz.timezone(tz)
    converted_dt = dt.astimezone(timezone)
    return converted_dt


def metric_report():          
    #Get timestamp
    date_now = date.today().strftime("%Y%m%d")
    time_now = datetime.datetime.now().time().strftime("%H:%M:%S")
    utc_dt_aware = datetime.datetime.now(datetime.timezone.utc)
    
    print("Getting realtime feeds")
    rt_mta_bus, rt_mta_mnr, rt_mta_lirr, rt_septa_bus, rt_septa_rail, rt_cta, rt_sf, rt_wmata_bus = rt_inputs.grab_rt_feeds()
    
    #Compile all the metrics
    timestamp = []
    region = []
    agency = []
    mode = []
    abs_delay = []
    early_delay = []
    late_delay = []
    otp = []
    fraction = []
    
    
    #Compile all of the metrics
    # New York
    if rt_mta_bus is not None:
        timestamp.append(timezone_converter(utc_dt_aware, "America/New_York"))
        region.append("nyc")
        agency.append("MTA")
        mode.append("Bus")
        abs_delay.append(avg_delay_abs(rt_mta_bus))
        early_delay.append(avg_delay_early(rt_mta_bus))
        late_delay.append(avg_delay_late(rt_mta_bus))
        otp.append(on_time_percent(rt_mta_bus))
        fraction.append(rt_fraction(rt_mta_bus))
    
    if rt_mta_mnr is not None:
        timestamp.append(timezone_converter(utc_dt_aware, "America/New_York"))
        region.append("nyc")
        agency.append("MNR")
        mode.append("Rail")
        abs_delay.append(avg_delay_abs(rt_mta_mnr))
        early_delay.append(avg_delay_early(rt_mta_mnr))
        late_delay.append(avg_delay_late(rt_mta_mnr))
        otp.append(on_time_percent(rt_mta_mnr))
        fraction.append(rt_fraction(rt_mta_mnr))
    
    if rt_mta_lirr is not None:
        timestamp.append(timezone_converter(utc_dt_aware, "America/New_York"))
        region.append("nyc")
        agency.append("LIRR")
        mode.append("Rail")
        abs_delay.append(avg_delay_abs(rt_mta_lirr))
        early_delay.append(avg_delay_early(rt_mta_lirr))
        late_delay.append(avg_delay_late(rt_mta_lirr))
        otp.append(on_time_percent(rt_mta_lirr))
        fraction.append(rt_fraction(rt_mta_lirr))

    
    #Philadelphia
    if rt_septa_bus is not None:
        timestamp.append(timezone_converter(utc_dt_aware, "America/New_York"))
        region.append("philiadelphia")
        agency.append("SEPTA")
        mode.append("Bus")
        abs_delay.append(avg_delay_abs(rt_septa_bus))
        early_delay.append(avg_delay_early(rt_septa_bus))
        late_delay.append(avg_delay_late(rt_septa_bus))
        otp.append(on_time_percent(rt_septa_bus))
        fraction.append(rt_fraction(rt_septa_bus))
    
    if rt_septa_rail is not None:
        timestamp.append(timezone_converter(utc_dt_aware, "America/New_York"))
        region.append("philiadelphia")
        agency.append("SEPTA")
        mode.append("Rail")
        abs_delay.append(avg_delay_abs(rt_septa_rail))
        early_delay.append(avg_delay_early(rt_septa_rail))
        late_delay.append(avg_delay_late(rt_septa_rail))
        otp.append(on_time_percent(rt_septa_rail))
        fraction.append(rt_fraction(rt_septa_rail))

    
    #Chicago
    if rt_cta is not None:
        timestamp.append(timezone_converter(utc_dt_aware, "America/Chicago"))
        region.append("chicago")
        agency.append("CTA")
        mode.append("Bus")
        abs_delay.append(np.nan)
        early_delay.append(np.nan)
        late_delay.append(np.nan)
        otp.append(100 - (rt_cta[rt_cta.delay == True].shape[0] / rt_cta.shape[0] * 100))
        fraction.append(np.nan)
    
    
    #Washington 
    if rt_wmata_bus is not None:
        timestamp.append(timezone_converter(utc_dt_aware, "America/New_York"))
        region.append("dc")
        agency.append("WMATA")
        mode.append("Bus")
        abs_delay.append(avg_delay_abs(rt_wmata_bus))
        early_delay.append(avg_delay_early(rt_wmata_bus))
        late_delay.append(avg_delay_late(rt_wmata_bus))
        otp.append(on_time_percent(rt_wmata_bus))
        fraction.append(rt_fraction(rt_wmata_bus))
    
    
    # San Francisco
    if rt_sf is not None:
        sf_agency_list = rt_sf['Id'].unique().tolist()
        for sf_agency in sf_agency_list:
            df_temp = rt_sf[rt_sf["Id"] == sf_agency].copy()
            if "Bus" in df_temp["mode"].values:
                timestamp.append(timezone_converter(utc_dt_aware, "America/Vancouver"))
                region.append("sf")
                agency.append(df_temp["Agency"].iloc[0])
                mode.append("Bus")
                abs_delay.append(avg_delay_abs(df_temp[df_temp["mode"] == "Bus"]))
                early_delay.append(avg_delay_early(df_temp[df_temp["mode"] == "Bus"]))
                late_delay.append(avg_delay_late(df_temp[df_temp["mode"] == "Bus"]))
                otp.append(on_time_percent(df_temp[df_temp["mode"] == "Bus"]))
                fraction.append(rt_fraction(df_temp[df_temp["mode"] == "Bus"]))
        
            if "Rail" in df_temp["mode"].values:
                timestamp.append(timezone_converter(utc_dt_aware, "America/Vancouver"))
                region.append("sf")
                agency.append(df_temp["Agency"].iloc[0])
                mode.append("Rail")
                abs_delay.append(avg_delay_abs(df_temp[df_temp["mode"] == "Rail"]))
                early_delay.append(avg_delay_early(df_temp[df_temp["mode"] == "Rail"]))
                late_delay.append(avg_delay_late(df_temp[df_temp["mode"] == "Rail"]))
                otp.append(on_time_percent(df_temp[df_temp["mode"] == "Rail"]))
                fraction.append(rt_fraction(df_temp[df_temp["mode"] == "Rail"]))
            
        
    reliability_metrics = {'Timestamp': timestamp, 
                           'Region' : region,
                           'Agency' : agency,
                           'Mode' : mode,
                           'Average delay absolute': abs_delay,
                           'Average delay late': late_delay,
                           'Average delay early': early_delay,
                           'On time performance': otp, 
                           'Fraction': fraction}

    reliability_metrics = pd.DataFrame(reliability_metrics)
    
    print("Printing out metrics")
    # Write out a csv file with the metrics
    reliability_metrics.to_csv('reliability_metric.csv', mode='a', header=False, index=False)   
    
    print("Metrics completed, sleep until next hour")
    
    #Make it sleep until the next hour if it is within the same hour
    curr_time = datetime.datetime.now().time().strftime("%H:%M:%S")
    hr_time = datetime.datetime.strptime(curr_time, '%H:%M:%S').time().hour
    min_time = datetime.datetime.strptime(curr_time, '%H:%M:%S').time().minute
    if hr_time == datetime.datetime.strptime(time_now, '%H:%M:%S').time().hour:
        sleep_time = (60 - min_time)*60
        if sleep_time < 0:
            sleep_time = 0
    else:
        sleep_time = 0
    time.sleep(sleep_time)
    
    schedule_next_run()
    return schedule.CancelJob


    
def metric_report3():          
    #Get timestamp
    date_now = date.today().strftime("%Y%m%d")
    time_now = datetime.datetime.now().time().strftime("%H:%M:%S")
    utc_dt_aware = datetime.datetime.now(datetime.timezone.utc)
    
    print("Getting realtime feeds")
    rt_mta_bus, rt_mta_mnr, rt_mta_lirr, rt_septa_bus, rt_septa_rail, rt_cta, rt_sf, rt_wmata_bus = rt_inputs.grab_rt_feeds()
    
    #Compile all the metrics
    timestamp = []
    region = []
    agency = []
    mode = []
    abs_delay = []
    early_delay = []
    late_delay = []
    otp = []
    fraction = []
    
    timestamp, region, mode, abs_delay, early_delay, late_delay, otp, fraction = metric_compile(rt_mta_bus,
                                                                                                utc_dt_aware,
                                                                                                timestamp,
                                                                                                "America/New_York",
                                                                                                region,
                                                                                                "New York",
                                                                                                "MTA",
                                                                                                mode,
                                                                                                abs_delay,
                                                                                                early_delay,
                                                                                                late_delay,
                                                                                                otp,
                                                                                                fraction)
    
    timestamp, region, mode, abs_delay, early_delay, late_delay, otp, fraction = metric_compile(rt_mta_mnr,
                                                                                                utc_dt_aware,
                                                                                                timestamp,
                                                                                                "America/New_York",
                                                                                                region,
                                                                                                "New York",
                                                                                                "MTA Metro-North",
                                                                                                mode,
                                                                                                abs_delay,
                                                                                                early_delay,
                                                                                                late_delay,
                                                                                                otp,
                                                                                                fraction)
    
    timestamp, region, mode, abs_delay, early_delay, late_delay, otp, fraction = metric_compile(rt_mta_lirr,
                                                                                                utc_dt_aware,
                                                                                                timestamp,
                                                                                                "America/New_York",
                                                                                                region,
                                                                                                "New York",
                                                                                                "MTA Long Island Rail Road",
                                                                                                mode,
                                                                                                abs_delay,
                                                                                                early_delay,
                                                                                                late_delay,
                                                                                                otp,
                                                                                                fraction)
    
    timestamp, region, mode, abs_delay, early_delay, late_delay, otp, fraction = metric_compile(rt_septa_bus,
                                                                                                utc_dt_aware,
                                                                                                timestamp,
                                                                                                "America/New_York",
                                                                                                region,
                                                                                                "New York/Philadelphia",
                                                                                                "SEPTA",
                                                                                                mode,
                                                                                                abs_delay,
                                                                                                early_delay,
                                                                                                late_delay,
                                                                                                otp,
                                                                                                fraction)
    
    timestamp, region, mode, abs_delay, early_delay, late_delay, otp, fraction = metric_compile(rt_septa_rail,
                                                                                                utc_dt_aware,
                                                                                                timestamp,
                                                                                                "America/New_York",
                                                                                                region,
                                                                                                "New York/Philadelphia",
                                                                                                "SEPTA",
                                                                                                mode,
                                                                                                abs_delay,
                                                                                                early_delay,
                                                                                                late_delay,
                                                                                                otp,
                                                                                                fraction)
    
    timestamp, region, mode, abs_delay, early_delay, late_delay, otp, fraction = metric_compile(rt_septa_bus,
                                                                                                utc_dt_aware,
                                                                                                timestamp,
                                                                                                "America/New_York",
                                                                                                region,
                                                                                                "New York/Philadelphia",
                                                                                                "SEPTA",
                                                                                                mode,
                                                                                                abs_delay,
                                                                                                early_delay,
                                                                                                late_delay,
                                                                                                otp,
                                                                                                fraction)
    
    timestamp, region, mode, abs_delay, early_delay, late_delay, otp, fraction = metric_compile(rt_septa_rail,
                                                                                            utc_dt_aware,
                                                                                            timestamp,
                                                                                            "America/New_York",
                                                                                            region,
                                                                                            "New York/Philadelphia",
                                                                                            "SEPTA",
                                                                                            mode,
                                                                                            abs_delay,
                                                                                            early_delay,
                                                                                            late_delay,
                                                                                            otp,
                                                                                            fraction)
    
    timestamp, region, mode, abs_delay, early_delay, late_delay, otp, fraction = metric_compile(rt_cta,
                                                                                            utc_dt_aware,
                                                                                            timestamp,
                                                                                            "America/Chicago",
                                                                                            region,
                                                                                            "Chicago",
                                                                                            "CTA",
                                                                                            mode,
                                                                                            abs_delay,
                                                                                            early_delay,
                                                                                            late_delay,
                                                                                            otp,
                                                                                            fraction)
    
    timestamp, region, mode, abs_delay, early_delay, late_delay, otp, fraction = metric_compile(rt_sf,
                                                                                            utc_dt_aware,
                                                                                            timestamp,
                                                                                            "America/Vancouver",
                                                                                            region,
                                                                                            "San Francisco",
                                                                                            "agency",
                                                                                            mode,
                                                                                            abs_delay,
                                                                                            early_delay,
                                                                                            late_delay,
                                                                                            otp,
                                                                                            fraction)
    
    timestamp, region, mode, abs_delay, early_delay, late_delay, otp, fraction = metric_compile(rt_wmata_bus,
                                                                                            utc_dt_aware,
                                                                                            timestamp,
                                                                                            "America/New_York",
                                                                                            region,
                                                                                            "Washington D.C.",
                                                                                            "WMATA",
                                                                                            mode,
                                                                                            abs_delay,
                                                                                            early_delay,
                                                                                            late_delay,
                                                                                            otp,
                                                                                            fraction)
    print(timestamp)
    print(region)
    print(mode)
    print(abs_delay)
        
    reliability_metrics = {'Timestamp': timestamp, 
                           'Region' : region,
                           'Agency' : agency,
                           'Mode' : mode,
                           'Average delay absolute': abs_delay,
                           'Average delay late': late_delay,
                           'Average delay early': early_delay,
                           'On time performance': otp, 
                           'Fraction': fraction}

    reliability_metrics = pd.DataFrame(reliability_metrics)
    
    print("Printing out metrics")
    # Write out a csv file with the metrics
    reliability_metrics.to_csv('reliability_metric.csv', mode='a', header=False, index=False)   
    
    print("Metrics completed, sleep until next hour")
    
    #Make it sleep until the next hour if it is within the same hour
    curr_time = datetime.datetime.now().time().strftime("%H:%M:%S")
    hr_time = datetime.datetime.strptime(curr_time, '%H:%M:%S').time().hour
    min_time = datetime.datetime.strptime(curr_time, '%H:%M:%S').time().minute
    if hr_time == datetime.datetime.strptime(time_now, '%H:%M:%S').time().hour:
        sleep_time = (60 - min_time)*60
        if sleep_time < 0:
            sleep_time = 0
    else:
        sleep_time = 0
    time.sleep(sleep_time)
    
    schedule_next_run()
    return schedule.CancelJob



def metric_report2():
    #Get timestamp
    date_now = date.today().strftime("%Y%m%d")
    time_now = datetime.datetime.now().time().strftime("%H:%M:%S")
    
    print("Getting active trips")
    # Get the realtime feeds and number of active trips at the time for all agency
    active_trips_subway, active_trips_bus, date_now, time_now = ny_active_trips.grab_active_trips()
    # Convert timestamp from string into datetime object
    timestamp_str = date_now + ' ' + time_now
    timestamp = datetime.datetime.strptime(timestamp_str, '%Y%m%d %H:%M:%S')
    
    print("Getting realtime feeds")
    df_rt_subway, df_rt_bus = ny_rt_inputs.grab_rt_feeds()
    
    if 'Agency' in df_rt_subway.columns:
        septa_count = df_rt_subway[df_rt_subway['Agency'] == 'SEPTA'].shape[0]
    else:
        septa_count = 0

    # Compare the number of promised trips to the number of actual trips
    missed_trips_bus = active_trips_bus.shape[0] - df_rt_bus.shape[0]
    missed_trips_subway = active_trips_subway.shape[0] - df_rt_subway.shape[0] - septa_count
    
    #Calculate the average delay and then summarize all metrics into one dataframe
    # On time performance window is -1 min and +5 min
    on_time_bus = df_rt_bus[(df_rt_bus['delay'] <= 300) & (df_rt_bus['delay'] >= -60)].shape[0]
    on_time_percent_bus = (on_time_bus / df_rt_bus['delay'].count()) * 100
    
    on_time_subway = df_rt_subway[(df_rt_subway ['delay'] <= 210) & (df_rt_subway ['delay'] >= -60)].shape[0]
    on_time_percent_subway  = (on_time_subway  / df_rt_subway ['delay'].count()) * 100
    
    avg_delay_abs_bus = df_rt_bus['delay'].mean()
    avg_delay_late_bus = df_rt_bus.delay[df_rt_bus['delay'] > 300].mean()
    avg_delay_early_bus = df_rt_bus.delay[df_rt_bus['delay'] < -60].mean()
    
    avg_delay_abs_subway = df_rt_subway['delay'].mean()
    avg_delay_late_subway = df_rt_subway.delay[df_rt_subway['delay'] > 300].mean()
    avg_delay_early_subway = df_rt_subway.delay[df_rt_subway['delay'] < -60].mean()
    
    # Get fraction of buses that do have delay information
    fraction_bus = (1 - df_rt_bus["delay"].isna().sum() / df_rt_bus.shape[0]) * 100
    fraction_subway = (1 - df_rt_subway["delay"].isna().sum() / df_rt_subway.shape[0]) * 100
    
    
    reliability_metrics = {'Timestamp':[timestamp], 
                           'Average delay absolute bus':[avg_delay_abs_bus],
                           'Average delay late bus': [avg_delay_late_bus],
                           'Average delay early bus': [avg_delay_early_bus],
                           'Missed trips bus': [missed_trips_bus],
                           'Fraction bus': [fraction_bus],
                           'Total scheduled trips bus': [active_trips_bus.shape[0]],
                           'On time performance bus': [on_time_percent_bus], 
                           'Average delay absolute subway':[avg_delay_abs_subway],
                           'Average delay late subway': [avg_delay_late_subway],
                           'Average delay early subway': [avg_delay_early_subway],
                           'Missed trips subway': [missed_trips_subway],
                           'Fraction subway': [fraction_subway],
                           'Total scheduled trips subway': [active_trips_subway.shape[0]],
                           'On time performance subway': [on_time_percent_subway]}
    
    reliability_metrics = pd.DataFrame(reliability_metrics)
    
    print("Printing out metrics")
    # Write out a csv file with the metrics
    reliability_metrics.to_csv('reliability_metric.csv', mode='a', header=False, index=False)
    df_rt_bus.to_csv('bus_delay.csv')
    df_rt_subway.to_csv('subway_delay.csv')
    
    
    #Make it sleep until the next hour if it is within the same hour
    curr_time = datetime.datetime.now().time().strftime("%H:%M:%S")
    hr_time = datetime.datetime.strptime(curr_time, '%H:%M:%S').time().hour
    min_time = datetime.datetime.strptime(curr_time, '%H:%M:%S').time().minute
    if hr_time == datetime.datetime.strptime(time_now, '%H:%M:%S').time().hour:
        sleep_time = (60 - min_time)*60
        if sleep_time < 0:
            sleep_time = 0
    else:
        sleep_time = 0
    time.sleep(sleep_time)
    
    schedule_next_run()
    return schedule.CancelJob
    


def schedule_next_run():
   time_str = ':{:02d}'.format(random.randint(0, 59))   
   schedule.clear()
   print("Scheduled for {}".format(time_str))
   schedule.every().hour.at(time_str).do(metric_report)


if __name__ == "__main__":
    schedule_next_run()
    while True:
        try:
            schedule.run_pending()
        except:
            print('Restarting script')
            curr_time = datetime.datetime.now().time().strftime("%H:%M:%S")
            min_time = datetime.datetime.strptime(curr_time, '%H:%M:%S').time().minute
            sleep_time = (60 - min_time)*60
            time.sleep(sleep_time) 
            pass
            
       
       