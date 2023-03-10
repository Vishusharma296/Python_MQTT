'''
   This script works as an MQTT subscriber.
   It records sensor data coming from LoRaWAN Gateway
   Gateway is sending data to the MQTT broker
   This Scripts was recording data packets as JSON objects
'''

# -------- Libraries and modules ----------

import logging
import json
import csv
import sys
import pandas as pd

import paho.mqtt.client as paho
import time
import datetime

def Message_handler(client, userdata, msg):
    topic = msg.topic
    msg_decoded = msg.payload.decode("utf-8", "ignore")
    msg_JSON = topic + ":" + msg_decoded
    print(msg_decoded)
    
    #-----Logging messages --------
    
    
    #---------- Text File ----------
    
    with open('Filename_dd.mm.yyyy.txt', 'a', encoding = 'utf-8') as f:
        f.write(msg_decoded)
        f.write(",")
        f.write('\n')


# ------- MQTT Connection handling ----------

MQTT_broker = "192.xx.xx.xx"    # Enter the IP of your broker
MQTT_port = 1883
keep_alive = 60
MQTT_topic = "#"                # Enter the topics you want to subscribe


#--------------------------------------------
client = paho.Client()
client.on_message = Message_handler

if client.connect(MQTT_broker, MQTT_port, keep_alive) != 0:
    print("Could not connect to MQTT broker... ")
    sys.exit()
    
    

client.subscribe(MQTT_topic)

# ---------- Data Logging --------------------

print("Successfully connected to broker ...")

try:
    client.loop_forever()
except:
    print("Disconnecting ...")



















 

