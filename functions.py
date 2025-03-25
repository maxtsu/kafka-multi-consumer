#!/usr/bin/python3
import json
import os
import re
from threading import Timer




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
