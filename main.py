#!/usr/bin/python3

#Importing Necessary Modules.
import signal
from kafka import KafkaConsumer
import os
import time #for testing
import logging
from logging.handlers import RotatingFileHandler
from functions import *

#getting Env details for TAND and group-id from ENV
tand_host = os.environ.get('TAND_HOST') + ".healthbot"
tand_port = os.environ.get('TAND_PORT')
pod_name = (os.environ.get('HOSTNAME')).split('-') #hostname used for consumer group-id
del pod_name[-2:]
group_id = ('-'.join(pod_name)) #group-id using hostanme

batch_size = 600 # Batch size for TSDB batch write
timeout = 1       # timeout for batchwrite [seconds]

### config file variable
configFile = '/etc/byoi/config.json'
caCertificate = '/ca.crt'

### Call function to extract logging level
level = loggingLevel(configFile)
logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=level)

### Call function to extract kafka brokers and topics
brokers, topics, user, password, saslmech, secprotocol = brokerTopic(configFile)
### split string with multiple brokers into a list 
brokers = brokers.split(",")

### Call function to parse the config.json file. Only happens once at start
### Pass file name into function
### Return tuple with name-path as key. values for each item
#       -namePath (combination of name & path for quick searching)
#       -deviceName
#       -sensor_name
#       -kvs_path
#       -measurement
#       -database
#       -rule-id (taken from parameters)
#       -hostname (extracted from database)
configuration = parseconfigJSON(configFile)

#SIGHUP to terminate the program
run = True
def handler_stop_signals(signum, frame):
    logging.critical('Received SIGHUP signal')
    global run
    run = False
signal.signal(signal.SIGHUP, handler_stop_signals)

### Create dictionary of writeBatch objects from device-DB list
### Added list_devices for kafka key matching
devices_databases, list_devices = configDatabases(configFile,timeout,batch_size)

### Check device list with key
def kafkaKeyCheck(byte_string):
    key = str(byte_string, encoding='utf-8') #convert to string
    ip = key.split(':') #Extract IP address from the key
    if ip[0] in list_devices:
        return True
    else:
        return False

#start of main script
while run:
    try:
        #Connect to Kafka broker and topic
        consumer = KafkaConsumer(topics,bootstrap_servers=brokers,group_id=group_id,sasl_plain_username=user,sasl_plain_password=password, \
              security_protocol=secprotocol,sasl_mechanism=saslmech, ssl_cafile=caCertificate)
        # iterate through kafka messages
        for msg in consumer:
            logging.debug('Received kafka message {}'.format(str(msg)[:400]))
            # Load Kafa message to Json
            telemetry_msg = msg.value
            if telemetry_msg is None: # Check for Null message
                logging.error('Null message Ignore {}'.format(str(msg)))
                continue
            if not kafkaKeyCheck(msg.key):
                continue # If device is not in keyCheck skip this iteration
            else:
                logging.info('Processing kafka message {}'.format(str(telemetry_msg)[:400]))
                # Call function to process the telemetry message
                processMessage(telemetry_msg,configuration,devices_databases)
            if not run:  # Break with SIGHUP
                logging.critical('Received SIGHUP breaking from kafka messaging loop closing consumer')
                #stop all running writeBatch objects
                for deviceName in devices_databases:
                    devices_databases[deviceName].stop()
                consumer.unsubscribe()
                consumer.close()
                break
    except Exception as e:
        logging.critical('Error in main loop: ' + str(e))
        run = False
logging.critical('Unsubscribe and close kafka consumer')
try:
    consumer.unsubscribe()
    consumer.close()
except:
    logging.critical('Exception in closing consumer')
logging.critical('Program terminated for restart')
