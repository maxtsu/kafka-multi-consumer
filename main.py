#!/usr/bin/python3

#Importing Necessary Modules.
import signal
import yaml
from kafka import KafkaConsumer
import time #for testing
from functions import *


configFile = 'kafka-config.yaml'
with open(configFile, 'r') as f:
    config = yaml.load(f, Loader=yaml.SafeLoader)

brokers     = config["bootstrap.servers"]
topics      = config["topics"]
user        = config["sasl.username"]
password    = config["sasl.password"]
secprotocol = config["security.protocol"]
saslmech    = config["sasl.mechanisms"]
caCertificate = config["ssl.ca.location"]
group_id    = config["group.id"]
offset      = config["auto.offset.reset"]


### Call function to extract kafka brokers and topics
# brokers, topics, user, password, saslmech, secprotocol = brokerTopic(configFile)
# ### split string with multiple brokers into a list 
brokers = brokers.split(",")

#SIGHUP to terminate the program
run = True
def handler_stop_signals(signum, frame):
    print('Received SIGHUP signal')
    global run
    run = False
signal.signal(signal.SIGHUP, handler_stop_signals)

#start of main script
while run:
    try:
        #Connect to Kafka broker and topic
        consumer = KafkaConsumer(topics,bootstrap_servers=brokers,group_id=group_id,sasl_plain_username=user,sasl_plain_password=password, \
              security_protocol=secprotocol,sasl_mechanism=saslmech, ssl_cafile=caCertificate)
        # iterate through kafka messages
        for msg in consumer:
            print('Received kafka message {}'.format(str(msg)[:400]))
            # Load Kafa message to Json
            telemetry_msg = msg.value
            if telemetry_msg is None: # Check for Null message
                print('Null message Ignore {}'.format(str(msg)))
                continue
            else:
                # Call function to process the telemetry message
                print('{} {} {}'.format(msg.offset, msg.partition, msg.key))
                print("Message is: {}".format(msg.value))
                print(msg)
            if not run:  # Break with SIGHUP
                print('Received SIGHUP breaking from kafka messaging loop closing consumer')
                consumer.unsubscribe()
                consumer.close()
                break
    except Exception as e:
        print('Error in main loop: ' + str(e))
        run = False
print('Unsubscribe and close kafka consumer')
try:
    consumer.unsubscribe()
    consumer.close()
except:
    print('Exception in closing consumer')
print('Program terminated for restart')
