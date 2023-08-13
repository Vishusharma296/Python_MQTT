'''
   MQTT Publisher - pseudosensor
   This script publishes the simulated data from the host computer (raspberrypi 400)
   Temperature, humidity and pressure data is published as seperate JSON strings
   MQTT broker and publishing client are assumed to be running on the same machine.

'''
import paho.mqtt.client as paho
import sys
import time
import datetime
from random import randrange, uniform
import json

client = paho.Client()


# Connection failure handling

MQTT_broker = "localhost"
MQTT_port = 1883
keep_alive = 60

if client.connect(MQTT_broker, MQTT_port, keep_alive) != 0:
    print("could not connect to MQTT broker!")
    sys.exit(-1)
    
    
print("Client connected to MQTT broker")

count = 1   # Counter for message Nr.

# This script publishes  different JSON objects containing
# Simulated data to different topics.
# Simulated contains temperature, humidity and pressure

while True:
    
    # Simulated data for temperature, humidity and pressure
    
    Sample_time = 30
    Temp_range = uniform(14.0, 17.0)
    Hum_range  = uniform(45.0, 52.0)
    Press_range = uniform(.9, 1.1)
    Time_stamp = str(datetime.datetime.now())
    
    MQTT_msg_temp = {
        
        "Message Nr." : count,
        "DevEUI"      : "PRXX00101",
        "Temperature" : Temp_range,
        "Location"    : "Boltzmann str 58",
        "Time_stamp"  : Time_stamp
        }
    
    MQTT_msg_hum = {
        
        "Message Nr." : count,
        "DevEUI"      : "PRXX00102",
        "Humidity"    : Hum_range,
        "Location"    : "Einstein str 106",
        "Time_stamp"  : Time_stamp
        }
    
    MQTT_msg_press = {
        
        "Message Nr." : count,
        "DevEUI"      : "PRXX00103",
        "Humidity"    : Press_range,
        "Location"    : "Berliner str 56",
        "Time_stamp"  : Time_stamp
        }
    
    JSON_msg_temp = json.dumps(MQTT_msg_temp, skipkeys = True, allow_nan = True, indent = 4)
    JSON_msg_hum = json.dumps(MQTT_msg_hum, skipkeys = True, allow_nan = True, indent = 4)
    JSON_msg_press = json.dumps(MQTT_msg_press, skipkeys = True, allow_nan = True, indent = 4)
    
# Publishing the pseudosensor data to different topics
    
    print ("Published Message Nr. ", count)
    
    print("Message published to topic: ", JSON_msg_temp)
    client.publish("Pseudo/temperature", JSON_msg_temp, 0)
    
    print("Message published to topic:  ", JSON_msg_hum)
    client.publish("Pseudo/humidity", JSON_msg_hum, 0)
    
    print("Message published to topic:  ", JSON_msg_press)
    client.publish("Pseudo/pressure", JSON_msg_press, 0)
    
    count = count + 1
    time.sleep(Sample_time)
    

