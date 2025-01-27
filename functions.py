#!/usr/bin/python3
import json
import os
import re
import logging
from threading import Timer
import queue
from influxdb import InfluxDBClient

#getting Env details for TAND from ENV
tand_host = os.environ.get('TAND_HOST') + ".healthbot"
tand_port = os.environ.get('TAND_PORT')

### Function to extract logging level from config.json
### Performed only at initialization
def loggingLevel(configFile):
    # logging level related attributes from config json
    with open(configFile, 'r') as f:
        config_json = json.load(f)
    level = str(config_json['logging']['level'].upper())
    return level

### Function to extract kafka brokers and topics from config.json
### Performed only at initialization
def brokerTopic(configFile):
    # read device, plugin, rule related attributes from config json
    with open(configFile, 'r') as f:
        config_json = json.load(f)
    for item in config_json['hbin']['inputs'][0]['plugin']['config']['kvs']:
        if item.get('key') == 'brokers':
            brokers = item.get('value')
        if item.get('key') == 'topics':
            topics = item.get('value')
        if item.get('key') == 'saslusername':
            user = item.get('value')
        if item.get('key') == 'saslpassword':
            password = item.get('value')
        if item.get('key') == 'saslmechanism':
            saslmech = item.get('value')
        if item.get('key') == 'securityprotocol':
            secprotocol = item.get('value')
    return brokers, topics, user, password, saslmech, secprotocol

### Parse the config.json file for device and measurements details
### Performed only at initialization
### Return tuple with name-path as key. values for each item
#       -namePath (combination of name & path for fast lookup)
#       -deviceName
#       -sensor_name
#       -kvs_path
#       -measurement
#       -database
#       -rule-id (taken from parameters)
#       -hostname (extracted from database name)
def parseconfigJSON(configFile):
    # read device, plugin, rule related attributes from config json
    with open(configFile, 'r') as f:
        config_json = json.load(f)
    intermediate_list = []
    for device_item in config_json['hbin']['inputs'][0]['plugin']['config']['device']:
        #Extract device ID
        deviceName = device_item['system-id']
        #Extract DB to store data
        database = device_item["healthbot-storage"]['database']
        #Extract the hostname from the database name
        hostname = (database.split(":"))[2]
        for sensor in device_item['sensor']:
            #Iterate over sensors
            sensor_name = sensor['name']
            #Extract measurement for point writing
            measurement = sensor['measurement']
            kvs_path = rule_id = 'Null'
            for kvs in sensor['kvs']:
                if kvs['key'] == 'path':
                    #Extract kvs path for sensor
                    kvs_path = kvs['value']
                if kvs['key'] == 'rule-id':
                    #Extract kvs path for rule-id
                    rule_id = kvs['value']
            #namepath is combination of name+path for faster searching in loops
            namePath = deviceName+"-"+kvs_path
            #all device details for sensor insert into tuple
            deviceKeyDetails = (namePath,deviceName,sensor_name,kvs_path,measurement,database,rule_id,hostname)
            #append tuple to the list
            intermediate_list.append(deviceKeyDetails)
    #Insert list into tuple
    configuration = tuple(intermediate_list)
    return configuration

### Function to read all devices and instantiate writeBuffer objects with database
### Return dictionary keys=DeviceName value=writeBuffer-object
def configDatabases(configFile,timeout,buffer_size):
    # read config.json file
    with open(configFile, 'r') as f:
        config_json = json.load(f)
    dict_databases = {}
    list_devices = [] #Create a list of devices
    for device_item in config_json['hbin']['inputs'][0]['plugin']['config']['device']:
        #Extract device ID
        deviceName = device_item['system-id']
        list_devices.append(deviceName) #Append devices to the list
        #Extract DB to store data
        database = device_item["healthbot-storage"]['database']
        #create object for writing to TSDB
        w = writeBatch(deviceName,database,timeout,buffer_size)
        w.start()
        dict_databases[deviceName] = w
    return dict_databases, list_devices

### Function to extract source path port key-value(from path) and timestamp from telemetry message
### Return tuple of source/name path port key_value and timestamp
def getSource(telemetry_msg_json):
    # Check if the Updates are present in json message
    if "updates" in telemetry_msg_json:
        #Get the Source address and port
        content_source = telemetry_msg_json['source']
        if ":" in content_source:
            #extract address and port number
            source = content_source.rsplit(":", 1)
        else:
            #if no port extract only address insert dummy port
            source = [content_source, "99999"]
        #extract path string from json
        #Get the Path
        update_path = telemetry_msg_json['updates'][0]['Path']
        #Get the Complete Path without []
        sensor_path_message = re.sub("[\[].*?[\]]", "", update_path)
        #Extracts dictionary of key and value pairs with associated folder name from the Path
        key_values = kvsExtract(update_path)
        #Extract device timestamp
        timestamp = telemetry_msg_json['timestamp']
        # Return a tuple:- source-name sensor-path source-port key_value timestamp
        return source[0], sensor_path_message, source[1], key_values, timestamp
    else:
        # No 'updates' in message
        return 'Null','Null',0,0,0

### Function to extract update path leaf values key-value(from path)
### returns a dictionary of {folder,{key1:value1,key2:value2},etc.}
def kvsExtract(update_path):
    dict_kvs={}
    #split into folders by forward slash Ignore slash inside square brackets
    folders = re.split(r'/(?=[^\]]*(?:\[|$))', update_path)
    #iterate into each folder
    path = ''
    for folder in folders:
        #determine number of key values by number of '['
        leafs = folder.count('[')
        #get the  path and add to exisiting path
        path = path + '/' + folder.split('[')[0]
        #number of kvs in folder defined the logic for extraction
        if leafs == 0:
            #nothing to do
            continue
        elif leafs == 1:
            subStr0 = folder.split('[')[1].split(']')[0]
            key, value =kvsParse(subStr0)
            keyValues = {key:value}
            dict_kvs[path[1:]] = keyValues
        elif leafs == 2:
            subStr0 = folder.split('[')[1].split(']')[0]
            subStr1 = folder.split('[')[2].split(']')[0]
            key, value =kvsParse(subStr0)
            keyValues = {key:value}
            key, value =kvsParse(subStr1)
            keyValues[key] = value
            dict_kvs[path[1:]] = keyValues
        elif leafs == 3:
            subStr0 = folder.split('[')[1].split(']')[0]
            subStr1 = folder.split('[')[2].split(']')[0]
            subStr2 = folder.split('[')[3].split(']')[0]
            key, value =kvsParse(subStr0)
            keyValues = {key:value}
            key, value =kvsParse(subStr1)
            keyValues[key] = value
            key, value =kvsParse(subStr2)
            keyValues[key] = value
            dict_kvs[path[1:]] = keyValues
        # Add extra leaf for 4 indexs in a level folder
        elif leafs == 4:
            subStr0 = folder.split('[')[1].split(']')[0]
            subStr1 = folder.split('[')[2].split(']')[0]
            subStr2 = folder.split('[')[3].split(']')[0]
            subStr3 = folder.split('[')[4].split(']')[0]
            key, value =kvsParse(subStr0)
            keyValues = {key:value}
            key, value =kvsParse(subStr1)
            keyValues[key] = value
            key, value =kvsParse(subStr2)
            keyValues[key] = value
            key, value =kvsParse(subStr3)
            keyValues[key] = value
            dict_kvs[path[1:]] = keyValues
        else:
            logging.debug('Unknown kvs pairs in update path {}'.format(update_path))
    return dict_kvs

### Extract Key-Value pair from leaf value
### split string around '='
def kvsParse(kvsString):
    key = kvsString.split('=')[0]
    value = kvsString.split('=')[1]
    return key, value

### Extract a KVS value from a defined folder
### This is from the key_values dictionary
def kvs_dict_parse(key_values, folder, key):
    if (folder in key_values):
        folder_dictionary = key_values[folder]
        if (key in folder_dictionary):
            return (folder_dictionary[key])
        else:
            return 'None'
    else:
        return 'None'

### Function to filter between raw & event gnmic JSON messages
###
def processMessage(telemetry_msg, configuration, devices_databases):
    logging.debug('Starting to process message {}'.format(str(telemetry_msg)[:500]))
    ### Telemetry Data message in JSON format
    telemetry_msg_json = json.loads(telemetry_msg)
    # Check if the Updates are present in json message
    if "updates" in telemetry_msg_json:
        #This is a raw gnmic message
        logging.debug('Starting to process raw message {}'.format(str(telemetry_msg)[:500]))
        processRawMessage(telemetry_msg, configuration, devices_databases)
    # Check if the Tags are present in json message
    if "tags" in telemetry_msg_json:
        #This is an event gnmic message
        logging.debug('Event format gnmic message: {}'.format(str(telemetry_msg)[:500]))
        processEventMessage(telemetry_msg_json, configuration, devices_databases)


### Function to process event gnmic kafka json message
###
def processEventMessage(telemetry_msg_json, configuration, devices_databases):
    logging.debug('Starting to process event message {}'.format(str(telemetry_msg_json)[:500]))
    ### Extract source, path, port, key_value and timestamp from message
    timestamp = telemetry_msg_json['timestamp']
    content_source = telemetry_msg_json['tags']['source']
    source = (content_source.rsplit(":", 1))[0]
    path = telemetry_msg_json['tags']['path']
    prefix = telemetry_msg_json['tags']['prefix']
    #create namePath key
    messageNamePath = source + '-' + path
    #Search for namePath
    matching_rule = 'Null'
    for item in configuration:
        if messageNamePath in item:
            matching_rule = item # Matching item from config.json
            #Match for cisco MDT storage telemetry
            if matching_rule[6] == 'ev-file-system-storage': #**** custom part        
                #Process cisco MDT storage Returns data package[] with multiple write-points
                data_package = eVmdtStorage(telemetry_msg_json,matching_rule,timestamp)
                for data in data_package: # Iterate to each data point in data package
                    # Select device and send the telemetry data to the object writeBuffer
                    if source in devices_databases: # verify database write object for source
                        devices_databases[source].addPoint(data) #Call method in object to append data
                        logging.info('Writing to TSDB: {}'.format(str(data)[:400]))
                    else:
                        logging.warning('No writeBuffer for: {}'.format(content_source))
                break #do not continue loop

# Function process event MDT storage
def eVmdtStorage(telemetry_msg_json,matching_rule,timestamp):
    # extract the index values from the message path string
    node_node_name = telemetry_msg_json['tags']['node_node-name']
    logging.info('Processing mdt storage message')
    base_folder = 'Cisco-IOS-XR-shellutil-filesystem-oper:/file-system/node/file-system.'
    #iterate across all the filesystems in MDT message
    data_package = []
    for i in range(8):
        num = str(i)
        if (base_folder+num+'/flags') in telemetry_msg_json['values']:
            fields = {} #initialize empty fields dictionary
            fields.update({'node-name':node_node_name})
            base_folder_drive = base_folder+num
            fields['flags'] = telemetry_msg_json['values'][base_folder_drive+'/flags']
            fields['free'] = telemetry_msg_json['values'][base_folder_drive+'/free']
            fields['prefixes'] = telemetry_msg_json['values'][base_folder_drive+'/prefixes']
            fields['size'] = telemetry_msg_json['values'][base_folder_drive+'/size']
            fields['type'] = telemetry_msg_json['values'][base_folder_drive+'/type']
            #Add device_hostname to the fields
            fields['device_hostname'] = matching_rule[7]
            #Create json data package for each filesystem
            data = {"measurement":matching_rule[4],"time":timestamp,"source":matching_rule[1],"fields":fields}
            data_package.append(data) #append each filesystem
        else:
            break
    return data_package

### Function to process kafka json message
###
def processRawMessage(telemetry_msg, configuration, devices_databases):
    logging.debug('Starting to process message {}'.format(str(telemetry_msg)[:500]))
    try:
        ### Telemetry Data message in JSON format
        telemetry_msg_json = json.loads(telemetry_msg)
        ### Extract source, path, port, key_value and timestamp from message
        ### ... key_values are a dictionary of {folder,{key1:value1,key2:value2},etc.} extracted from path of message
        ### e.g. 'interfaces/interface[name=ge-0/0/1]/state' interface name ge-0/0/1 is in folder 'interface'
        content_source, sensor_path_message, port, key_values, timestamp = getSource(telemetry_msg_json)
        ### Create name-path string for matching configuration tuple
        messageNamePath = content_source + '-' + sensor_path_message
        ### Search configuration for this name-path pair
        ### Check tuple inside tuple
        ### If there is no match to configuration tuples. Message is ignored
        matching_rule = 'Null'
        for item in configuration:
            if messageNamePath in item:
                matching_rule = item # Matching item from config.json
                #Match for cisco MDT storage telemetry
                if matching_rule[6] == 'file-system-storage': #**** custom part
                    #Process cisco MDT storage Returns data package[] with multiple write-points
                    data_package = mdtStorage(telemetry_msg_json,matching_rule,key_values,timestamp)
                    for data in data_package: # Iterate to each data point in data package
                        # Select device and send the telemetry data to the object writeBuffer
                        if content_source in devices_databases: # verify database write object for source
                            devices_databases[content_source].addPoint(data) #Call method in object to append data
                            logging.info('Writing to TSDB: {}'.format(str(data)[:400]))
                        else:
                            logging.warning('No writeBuffer for: {}'.format(content_source))
                    break #do not continue loop
                #Match for cisco MDT optical telemetry
                elif matching_rule[6] == 'optics-power':
                    #Process cisco MDT optical power Returns single data write-points
                    data = opticalPower(telemetry_msg_json,matching_rule,key_values,timestamp)
                    if not 'Null' in data['fields']: # As long as there is no 'Null' in fields write to TSDB
                        if content_source in devices_databases: # verify database write object for source
                            devices_databases[content_source].addPoint(data) #Call method in object to append data
                            logging.info('Writing to TSDB: {}'.format(str(data)[:400]))
                        else:
                            logging.warning('No writeBuffer for: {}'.format(content_source))
                #Match for cisco MDT cos input queue telemetry
                elif matching_rule[6] == 'cos-queue-drops-input': #**** custom part
                    #Process cisco MDT storage Returns data package[] with multiple write-points
                    data_package = cos_queue_drops_input(telemetry_msg_json,matching_rule,key_values,timestamp)
                    for data in data_package: # Iterate to each data point in data package
                        # Select device and send the telemetry data to the object writeBuffer
                        if content_source in devices_databases: # verify database write object for source
                            #print("content-",content_source)
                            devices_databases[content_source].addPoint(data) #Call method in object to append data
                            logging.info('Writing to TSDB: {}'.format(str(data)[:400]))
                        else:
                            logging.warning('No writeBuffer for: {}'.format(content_source))
                    break #do not continue loop
                #Match for cisco MDT cos input queue telemetry
                elif matching_rule[6] == 'cos-queue-drops-output': #**** custom part
                    #Process cisco MDT storage Returns data package[] with multiple write-points
                    data_package = cos_queue_drops_output(telemetry_msg_json,matching_rule,key_values,timestamp)
                    for data in data_package: # Iterate to each data point in data package
                        # Select device and send the telemetry data to the object writeBuffer
                        if content_source in devices_databases: # verify database write object for source
                            #print("content-",content_source)
                            devices_databases[content_source].addPoint(data) #Call method in object to append data
                            logging.info('Writing to TSDB: {}'.format(str(data)[:400]))
                        else:
                            logging.warning('No writeBuffer for: {}'.format(content_source))
                    break #do not continue loop
                else:
                    continue # continue with next iteration for a match
        if matching_rule == 'Null':
            # log ignored message
            logging.info('Ignore kafka message {}'.format(str(telemetry_msg)[:400]))
    except json.JSONDecodeError as json_error: #exception in json.loads()
        logging.error('Invalid JSON syntax: {}. Invalid JSON message: {}'.format(json_error, (str(telemetry_msg)[:400])))

# Function to write to TSDB
def writeTSDB(database, json_body):
    # Data to be written to influx
    client = InfluxDBClient(host=tand_host, port=tand_port, database=database, timeout=100)
    write = client.write_points(json_body)
    logging.debug('Writing batch to Influx DB: {} {}'.format(database, write))

# Function process MDT optical power
def opticalPower(telemetry_msg_json,matching_rule,key_values,timestamp):
    fields = {} #initialize empty fields dictionary
    logging.info('Processing mdt optical power message')
    # extract the index values from the message path string
    index = {'path':'optics-oper/optics-ports/optics-port','index':'name'}
    value = kvs_dict_parse(key_values, index['path'] , index['index'])
    path = index['path'] + '/' + index['index']
    fields.update({path:value}) # Add path and index value to fields data
    base_folder = telemetry_msg_json['updates'][0]['values']
    #iterate across 'optics-oper/optics-ports/optics-port/optics-info' folder in MDT message
    optics_folder = base_folder['optics-oper/optics-ports/optics-port/optics-info']
    for val in ['rx-high-threshold','rx-low-threshold','tx-high-threshold','tx-low-threshold']:
        #iterate across value items for data points
        if val in optics_folder: #dict key verification
            # If the key exists in the dictionary, we will extract for point writing
            fields[val] = optics_folder[val]
        else: #the data field in rules.json not in the message
            # If the parameter does not exist raise message
            logging.error('Key \'{}\' not in kafka message'.format(val))
            fields['Null'] = 'Null'
    # Data from lane-data folder - Check if folder exists
    if 'lane-data' in base_folder['optics-oper/optics-ports/optics-port/optics-info']:
        #iterate across lane_data list [0] in MDT message
        lane_folder = base_folder['optics-oper/optics-ports/optics-port/optics-info']['lane-data'][0]
        for val in ['lane-index','receive-power','transmit-power']:
            #iterate across value items for data points
            if val in lane_folder: #dict key verification
                # If the key exists in the dictionary, we will extract for point writing
                fields[val] = lane_folder[val]
            else: #the data field in rules.json not in the message
                # If the parameter does not exist raise message
                logging.error('Key \'{}\' not in kafka message'.format(val))
                fields['Null'] = 'Null'
    else: #No lane-data in message
        logging.info('No lane-data in optics message: {}'.format(str(telemetry_msg_json)[:500]))
        logging.debug('The optical message does not contain lane-data: {}'.format(str(telemetry_msg_json)))
        fields['Null'] = 'Null'
    #Add device_hostname to the fields
    fields['device_hostname'] = matching_rule[7]    
    #Create json data for each
    data = {"measurement":matching_rule[4],"time":timestamp,"source":matching_rule[1],"fields":fields}
    return data

# Function process MDT storage
def mdtStorage(telemetry_msg_json,matching_rule,key_values,timestamp):
    # extract the index values from the message path string
    index = {'path':'file-system/node','index':'node-name'}
    value = kvs_dict_parse(key_values, index['path'] , index['index'])
    path = index['path'] + '/' + index['index']
    logging.info('Processing mdt storage message')
    base_folder = telemetry_msg_json['updates'][0]['values']
    filesystems = base_folder["file-system/node"]["file-system"]
    #iterate across all the filesystems in MDT message
    data_package = []
    for filesystem in filesystems:
        fields = {} #initialize empty fields dictionary
        for val in ["flags","free","prefixes","size","type"]:
            #iterate across value items for data points
            if val in filesystem: #dict key verification
                # If the key exists in the dictionary, we will extract for point writing
                fields[val] = filesystem[val]
            else: #the data field in rules.json not in the message
                # If the parameter does not exist raise message
                logging.error('Key \'{}\' not in kafka message'.format(val))
                fields['Null'] = 'Null'
        fields.update({path:value}) # Add path and index value (node-name) to fields data
        #Add device_hostname to the fields
        fields['device_hostname'] = matching_rule[7]
        #Create json data package for each filesystem
        data = {"measurement":matching_rule[4],"time":timestamp,"source":matching_rule[1],"fields":fields}
        data_package.append(data) #append each filesystem
    return data_package

# Function process MDT COS Queue input
def cos_queue_drops_input(telemetry_msg_json,matching_rule,key_values,timestamp):
    #fields = {} #initialize empty fields dictionary
    data_package = []
    # extract the index values from the message path string
    index = {'path':'qos/nodes/node/policy-map/interface-table/interface','index':'interface-name'}
    value = kvs_dict_parse(key_values, index['path'], index['index'])
    path = index['path'] + '/' + index['index']
    logging.info('Processing input cos queue message')
    base_folder = telemetry_msg_json['updates'][0]['values']['qos/nodes/node/policy-map/interface-table/interface/input/service-policy-names/service-policy-instance/statistics']
    #iterate across 'qos/nodes/node/policy-map/interface-table/interface/input/service-policy-names/service-policy-instance/statistics' folder in MDT message
    class_iterate = base_folder['class-stats']
    if "child-policy" in class_iterate[0]:
        class_iterate = base_folder['class-stats'][0]['child-policy']['class-stats']
        for class_name in class_iterate:
            fields ={}
            for val in ["class-name"]:
                # iterate across value items for data points
                if val in class_name:  # dict key verification
                    # If the key exists in the dictionary, we will extract for point writing
                    fields[val] = class_name[val]
                class_iterate_drop_stats = class_name['general-stats']
                for stats in class_iterate_drop_stats:
                    for val in ['total-drop-bytes', 'total-drop-packets', 'total-drop-rate']:
                        # iterate across value items for data points
                        if val in stats:  # dict key verification
                            # If the key exists in the dictionary, we will extract for point writing
                            fields[val] = class_iterate_drop_stats[val]
            fields.update({path:value}) # Add path and index value (node-name) to fields data
            #Add device_hostname to the fields
            fields['device_hostname'] = matching_rule[7]
            #Create json data package for each filesystem
            data = {"measurement":matching_rule[4],"time":timestamp,"source":matching_rule[1],"fields":fields}
            data_package.append(data)  # append each class drops stats
        return data_package
    else:
        class_iterate = base_folder['class-stats']
        for class_name in class_iterate:
            fields = {}
            for val in ["class-name"]:
                if val in class_name:  # dict key verification
                    # If the key exists in the dictionary, we will extract for point writing
                    fields[val] = class_name[val]
                class_iterate_drop_stats = class_name['general-stats']
                for stats in class_iterate_drop_stats:
                    for val in ['total-drop-bytes', 'total-drop-packets', 'total-drop-rate']:
                        # iterate across value items for data points
                        if val in stats:  # dict key verification
                            # If the key exists in the dictionary, we will extract for point writing
                            fields[val] = class_iterate_drop_stats[val]
            fields.update({path:value}) # Add path and index value (node-name) to fields data
            #Add device_hostname to the fields
            fields['device_hostname'] = matching_rule[7]
            #Create json data package for each filesystem
            data = {"measurement":matching_rule[4],"time":timestamp,"source":matching_rule[1],"fields":fields}
            data_package.append(data)  # append each class drops stats
        return data_package

# Function process MDT COS Queue output
def cos_queue_drops_output(telemetry_msg_json,matching_rule,key_values,timestamp):
    #fields = {} #initialize empty fields dictionary
    data_package = []
    # extract the index values from the message path string
    index = {'path':'qos/nodes/node/policy-map/interface-table/interface','index':'interface-name'}
    value = kvs_dict_parse(key_values, index['path'], index['index'])
    path = index['path'] + '/' + index['index']
    logging.info('Processing output cos queue message')
    base_folder = telemetry_msg_json['updates'][0]['values']['qos/nodes/node/policy-map/interface-table/interface/output/service-policy-names/service-policy-instance/statistics']
    #iterate across 'qos/nodes/node/policy-map/interface-table/interface/output/service-policy-names/service-policy-instance/statistics' folder in MDT message
    class_iterate = base_folder['class-stats']
    if "child-policy" in class_iterate[0]:
        class_iterate = base_folder['class-stats'][0]['child-policy']['class-stats']
        for class_name in class_iterate:
            fields ={}
            for val in ["class-name"]:
                # iterate across value items for data points
                if val in class_name:  # dict key verification
                    # If the key exists in the dictionary, we will extract for point writing
                    fields[val] = class_name[val]
                class_iterate_drop_stats = class_name['general-stats']
                for stats in class_iterate_drop_stats:
                    for val in ['total-drop-bytes', 'total-drop-packets', 'total-drop-rate']:
                        # iterate across value items for data points
                        if val in stats:  # dict key verification
                            # If the key exists in the dictionary, we will extract for point writing
                            fields[val] = class_iterate_drop_stats[val]
            fields.update({path:value}) # Add path and index value (node-name) to fields data
            #Add device_hostname to the fields
            fields['device_hostname'] = matching_rule[7]
            #Create json data package for each filesystem
            data = {"measurement":matching_rule[4],"time":timestamp,"source":matching_rule[1],"fields":fields}
            data_package.append(data)  # append each class drops stats
        return data_package
    else:
        class_iterate = base_folder['class-stats']
        for class_name in class_iterate:
            fields = {}
            for val in ["class-name"]:
                if val in class_name:  # dict key verification
                    # If the key exists in the dictionary, we will extract for point writing
                    fields[val] = class_name[val]
                class_iterate_drop_stats = class_name['general-stats']
                for stats in class_iterate_drop_stats:
                    for val in ['total-drop-bytes', 'total-drop-packets', 'total-drop-rate']:
                        # iterate across value items for data points
                        if val in stats:  # dict key verification
                            # If the key exists in the dictionary, we will extract for point writing
                            fields[val] = class_iterate_drop_stats[val]
            fields.update({path:value}) # Add path and index value (node-name) to fields data
            #Add device_hostname to the fields
            fields['device_hostname'] = matching_rule[7]
            #Create json data package for each filesystem
            data = {"measurement":matching_rule[4],"time":timestamp,"source":matching_rule[1],"fields":fields}
            data_package.append(data)  # append each class drops stats
        return data_package

### Object class to batch point writes to Influxdb  Object instantiated for each device/DB
###
class writeBatch():
    def __init__(self,device,database,timeout,batch_size):
        self.qu = queue.Queue()
        self.device = device
        self.database = database
        self.timeout = timeout
        self.batch_size = batch_size
        self.running = True
        self.start()
    def _run(self):
        self.write()
        self.start()
    def stop(self):
        self.running = False
        self._timer.cancel()
        self.write() # Flush the queue
        logging.critical('Stopping writeBatch object for {}'.format(self.device))
    def addPoint(self, fields):
        try:
            #logging.debug('Object addPoint called')
            self.qu.put_nowait(fields) # add point to the queue
        except Exception as e:
            logging.debug('Not able to add queue full {} '.format(e))
        #logging.debug('Points added to DB {} '.format(self.device,str(fields)[:400]))
    def start(self):
        if self.running:
            self._timer = Timer(self.timeout, self._run)
            self._timer.start() #start timer
    def write(self):
        self._empty = self._queue_empty() # empty queue True/False
        while not self._empty: #if queue is not empty
            self.json_body = []
            self.n = self.thisqsize
            if self.thisqsize >= self.batch_size:
                self.n = self.batch_size
            else:
                self.n = self.thisqsize
            for _ in range(self.n):
                #self.json_body.append(self.qu.get())
                try:
                    self.json_body.append(self.qu.get_nowait())
                except Exception as e:
                    print("Queue empty")
            writeTSDB(self.database, self.json_body)
            self._empty = self._queue_empty() # Check if the queue is empty
    def _queue_empty(self):
        self.thisqsize = self.qu.qsize() # Set current queue size
        return not(self.thisqsize>0) # Check if the queue is empty
