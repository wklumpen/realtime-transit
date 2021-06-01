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
import traceback

sys.path.append(".")
sys.path.append("../equity-pulse-db")

from db import Realtime, Log


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
    Log.info(function='reliability', message="I'm awake! Running realtime metrics")
    rt_mta_bus, rt_mta_mnr, rt_mta_lirr, rt_septa_bus, rt_septa_rail, rt_cta, rt_sf, rt_wmata_bus = rt_inputs.grab_rt_feeds()
    Log.debug(function='reliability', message='Fetched realtime feeds')
    
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
        region.append("philadelphia")
        agency.append("SEPTA")
        mode.append("Bus")
        abs_delay.append(avg_delay_abs(rt_septa_bus))
        early_delay.append(avg_delay_early(rt_septa_bus))
        late_delay.append(avg_delay_late(rt_septa_bus))
        otp.append(on_time_percent(rt_septa_bus))
        fraction.append(rt_fraction(rt_septa_bus))
    
    if rt_septa_rail is not None:
        timestamp.append(timezone_converter(utc_dt_aware, "America/New_York"))
        region.append("philadelphia")
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
            
        
    reliability_metrics = {'timestamp': timestamp, 
                           'region' : region,
                           'agency' : agency,
                           'mode' : mode,
                           'delay_abs': abs_delay,
                           'delay_late': late_delay,
                           'delay_early': early_delay,
                           'otp': otp, 
                           'fraction': fraction}

    reliability_metrics = pd.DataFrame(reliability_metrics)
    
    # TODO: Do some cleaning here!

    reliability_metrics['delay_abs'] = reliability_metrics['delay_abs'].where(
        reliability_metrics['delay_abs'].astype(float) > -1800)
    reliability_metrics['delay_early'] = reliability_metrics['delay_early'].where(
        reliability_metrics['delay_early'].astype(float) < 1800)

    reliability_metrics['delay_late'] = reliability_metrics['delay_late'].where(
        reliability_metrics['delay_late'].astype(float) < 1800)

    reliability_metrics['otp'] = reliability_metrics['otp'].where(
        reliability_metrics['otp'].astype(float) > 20)

    # Write out a csv file with the metrics
    reliability_metrics.to_csv('reliability_metric.csv', mode='a', header=False, index=False)
    
    # Convert to none data types
    reliability_metrics = reliability_metrics.where(pd.notnull(reliability_metrics), None)
    Log.debug(function='reliability', message="Appended metrics to file")

    # Put it into the database 
    for idx, row in reliability_metrics.iterrows():
        Realtime.create(
            timestamp=row['timestamp'],
            region=row['region'],
            agency=row['agency'],
            mode=row['mode'],
            delay_abs=row['delay_abs'],
            delay_early=row['delay_early'],
            delay_late=row['delay_late'],
            otp=row['otp'],
            fraction=row['fraction']
        )
    Log.info(function='reliability', message="Inserted reliability metrics")
    
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
#    time_str = ':10'   
   schedule.clear()
   Log.debug(function='reliability', message=f"Scheduled for {time_str}")
   schedule.every().hour.at(time_str).do(metric_report)


if __name__ == "__main__":
    schedule_next_run()
    while True:
        try:
            schedule.run_pending()
        except Exception:
            # Script crashed, lets restart it!

            traceback.print_exc()
            exc_type, exc_value, exc_traceback = sys.exc_info()
            Log.error('realtime', '\n'.join(traceback.format_exception_only(exc_type, exc_value)))
            print('Restarting script')
            curr_time = datetime.datetime.now().time().strftime("%H:%M:%S")
            min_time = datetime.datetime.strptime(curr_time, '%H:%M:%S').time().minute
            sleep_time = (60 - min_time)*60
            time.sleep(sleep_time) 
            pass
            
       
       