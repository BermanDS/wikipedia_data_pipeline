##### This is the module of processing streaming data. #####################################################
# Author:        Anton Salyaev                                                                             #
# Email:         asalyaev@corp.finam.ru                                                                    #
# Description:   Necessary procedures and classes of Kafka connector and main object of data processing    #
############################################################################################################# 
import re
import gc
import os
import ast
import sys
import json
import pytz
import uuid
import asyncio
import aiohttp
import logging
import requests
import itertools
import numpy as np
import pandas as pd
from importlib import reload
from time import time, sleep
from dateutil.parser import parse
from datetime import datetime, timedelta, date, timezone
from kafka import KafkaProducer, KafkaConsumer, TopicPartition


class NpEncoder(json.JSONEncoder):
    """ Custom encoder for numpy data types """
    
    def default(self, obj):
        if isinstance(obj, (np.int_, np.intc, np.intp, np.int8,
                            np.int16, np.int32, np.int64, np.uint8,
                            np.uint16, np.uint32, np.uint64)):

            return int(obj)

        elif isinstance(obj, (np.float_, np.float16, np.float32, np.float64)):
            return float(obj)

        elif isinstance(obj, (np.complex_, np.complex64, np.complex128)):
            return {'real': obj.real, 'imag': obj.imag}

        elif isinstance(obj, (np.ndarray,)):
            return obj.tolist()

        elif isinstance(obj, (np.bool_)):
            return bool(obj)

        elif isinstance(obj, (np.void)): 
            return None
        
        elif isinstance(obj, datetime):
            return obj.isoformat()
        
        return json.JSONEncoder.default(self, obj)


def json_extract(obj, key):
    """Recursively fetch values from nested JSON."""
    arr = []

    def extract(obj, arr, key):
        """Recursively search for values of key in JSON tree."""
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, (dict, list)):
                    extract(v, arr, key)
                elif k == key:
                    arr.append(v)
        elif isinstance(obj, list):
            for item in obj:
                extract(item, arr, key)
        return arr

    values = extract(obj, arr, key)
    return values

####### Kafka connector ###################################################################################

class DBkafka:
    """
    KAFKA Database class.
    """

    def __init__(self, 
                 topic: str = '',
                 host: str = None,
                 username: str = None,
                 password: str = None, 
                 port: int = 9092,
                 tz: str = None, 
                 log_path: str = os.getcwd(),
                 bootstrap_servers: list = [],
                 log_level: str = 'error',
                 headers: dict = {},
                 consumer_offset_reset: str = 'latest', 
                 api_version: tuple = (2,10), 
                 security_protocol: str = 'PLAINTEXT',\
                 auth_mechanizm: str = 'PLAIN',
                 value_deserializer = None,
                 value_serializer = None):

        self.topic = topic
        self.timezone = tz
        self.headers = headers
        self.log_path = log_path
        self.offset_reset = consumer_offset_reset
        self.host = host
        self.bootstrap_servers = bootstrap_servers
        self.security_protocol = security_protocol
        self.api_version = api_version
        self.user = username
        self.password = password
        self.port = port
        self.batch_size = 1
        self.df_result = pd.DataFrame()
        self.dc_result = {}
        self.format_dt = '%Y-%m-%d %H:%M:%S'
        self.auth_mechanizm = auth_mechanizm
        self.value_deserializer = value_deserializer
        self.value_serializer = value_serializer
        self.conn_cons = None
        self.conn_prod = None
        self.log_level = log_level
        self.install_tz = True
        self.localtz = pytz.timezone(self.timezone) if self.timezone else pytz.timezone('Etc/GMT')
        self.log_levels = ["debug", "info", "warn", "error"]
        self._logger_init()

    
    def _logger_init(self):

        reload(logging)
        location = f'kafka_for_{self.host}' if self.bootstrap_servers == [] else f'kafka_for_{self.bootstrap_servers[0]}'
        
        logging.basicConfig(
            filename = os.path.join(self.log_path, f'{location}.log'),
            level = logging.INFO if self.log_level == 'info' \
                    else logging.ERROR if self.log_level == 'error' \
                    else logging.WARN if self.log_level == 'warn' \
                    else logging.DEBUG,
        )
        self.logger = logging.getLogger('KAFKA_BROKER')

    
    def log(self, tag: str = 'kafka-service', log_level: str = 'info', message: str = '', data: dict = {}) -> None:

        if self.log_levels.index(log_level) >= self.log_levels.index(self.log_level):
            log_info = {
                "level": log_level,
                "time": datetime.now(timezone.utc).isoformat(),
                "tag": tag,
                "message": message,
            }
            log_info.update(data)
            
            if log_level == 'info': self.logger.info(json.dumps(log_info, cls=NpEncoder))
            elif log_level == 'debug':self.logger.debug(json.dumps(log_info, cls=NpEncoder))
            elif log_level == 'warn': self.logger.warn(json.dumps(log_info, cls=NpEncoder))
            elif log_level == 'error':self.logger.error(json.dumps(log_info, cls=NpEncoder))
    

    def connect_producer(self):
        """
        Connect to a Kafka broker as Producer.
        """
        
        if self.headers == {}:
            self.headers['version'] = '-1'

        if self.conn_prod is None:
            try:
                self.conn_prod = KafkaProducer(
                                    bootstrap_servers=[f'{self.host}:{self.port}'] if self.bootstrap_servers == [] else self.bootstrap_servers,
                                    api_version=self.api_version,
                                    value_serializer = self.value_serializer,
                                    security_protocol=self.security_protocol,
                                    sasl_mechanism=self.auth_mechanizm,
                                    sasl_plain_username=self.user,
                                    sasl_plain_password=self.password,
                                    )
            except Exception as error:
                self.log('connection to Broker', 'error', f"Connection to Bootstrap {self.host}: {error}")

    
    def connect_consumer(self):
        """
        Connect to a Kafka broker as Consumer.
        """
        
        if self.conn_cons is None:
            try:
                self.conn_cons = KafkaConsumer(
                                    auto_offset_reset=self.offset_reset,
                                    bootstrap_servers=[f'{self.host}:{self.port}'] if self.bootstrap_servers == [] else self.bootstrap_servers,
                                    api_version=self.api_version,
                                    value_deserializer = self.value_deserializer,
                                    consumer_timeout_ms=1000,
                                    security_protocol=self.security_protocol,
                                    sasl_mechanism=self.auth_mechanizm,
                                    sasl_plain_username=self.user,
                                    sasl_plain_password=self.password,
                                    )
            except Exception as error:
                self.log('connection to Broker', 'error', f"Connection to Bootstrap {self.host}: {error}")
                

    def publish_message(self, key, value):

        
        self.connect_producer()

        try:
            partitions = [x for x in self.conn_prod.partitions_for(self.topic)]
            #------------------------------------------------------------------------------------
            self.conn_prod.send(self.topic,\
                                key=key,\
                                value=value,\
                                headers=[(k,str(v).encode()) for k,v in self.headers.items()],\
                                partition=np.random.choice(partitions))
            #-------------------------------------------------------------------------------------
            self.conn_prod.flush()
        except Exception as error:
            self.log('publishing to Broker', 'error', f"Publishing message to topic {self.topic}: {error}")
                

    def reading_que(self, partition_offset : dict, to_frame: bool = True) -> dict:
        """
        messages from que for transform to dict or dataframe
        result of consumering is in internal variable 
        """

        self.connect_consumer()
        self.dc_result, new_partition_offset = {}, {}
        self.df_result = pd.DataFrame()

        try:
            partitions = self.conn_cons.partitions_for_topic(self.topic)
            
            for p in partitions:
                tp = TopicPartition(self.topic, p)
                self.conn_cons.assign([tp])
                self.conn_cons.seek_to_beginning(tp)
                if p in partition_offset.keys():
                    if self.conn_cons.beginning_offsets([tp])[tp] <= partition_offset[p]:
                        self.conn_cons.seek(tp, int(partition_offset[p]))
                offset = self.conn_cons.position(tp)

                self.log('consuming from Broker','info', f"Start consuming partition {p} in topic {self.topic} from offset {offset}")
                
                for msg in self.conn_cons:
                    self.dc_result[f"{msg.partition}_{msg.offset}"] = {}
                    self.dc_result[f"{msg.partition}_{msg.offset}"]['key'] = msg.key if msg.key is None else msg.key.decode()
                    self.dc_result[f"{msg.partition}_{msg.offset}"]['value'] = msg.value
                    self.dc_result[f"{msg.partition}_{msg.offset}"]['date'] = datetime.fromtimestamp(msg.timestamp/1000)
                    self.dc_result[f"{msg.partition}_{msg.offset}"]['header'] = msg.headers

                new_partition_offset[p] = self.conn_cons.position(tp)

            if to_frame:
                self.df_result = pd.DataFrame(self.dc_result).T.reset_index()
        except Exception as error:
            new_partition_offset = partition_offset
            self.log('consuming from Broker','error', f"reading que from topic {self.topic}: {error}")
            
        self.close_cons()
        
        return new_partition_offset
        
    
    def close_cons(self):

        if self.conn_cons:
            self.conn_cons.close()
            self.conn_cons = None


    def close_prod(self):

        if self.conn_prod:
            self.conn_prod.close()
            self.conn_prod = None