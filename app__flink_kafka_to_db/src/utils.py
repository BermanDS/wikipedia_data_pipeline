import os
import datetime as dt
import bs4
import json
import socket
import logging
from importlib import reload
import numpy as np
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, DataTypes, EnvironmentSettings, TableEnvironment
from pyflink.table.udf import udf


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

log_levels = ["debug", "info", "warn", "error"]

###################################################################################################
### Logging
###################################################################################################

def logger_init(location: str = '', log_path: str = os.getcwd(), log_level: str = 'info') -> object:

    reload(logging)
    location = location if location != '' else socket.gethostname()

    logging.basicConfig(
            filename = os.path.join(log_path, f'{location}.log'),
            level = logging.INFO if log_level == 'info' \
                    else logging.ERROR if log_level == 'error' \
                    else logging.WARN if log_level == 'warn' \
                    else logging.DEBUG,
        )
    return logging.getLogger('APP_FLINK')


def log(logger: object = None, tag: str = 'flink-service', log_level: str = 'info', message: str = '', data: dict = {}) -> None:

    if log_levels.index(log_level) >= log_levels.index(log_level) and logger:
        log_info = {
            "level": log_level,
            "time": datetime.now(timezone.utc).isoformat(),
            "tag": tag,
            "message": message,
        }
        log_info.update(data)
            
        if log_level == 'info': logger.info(json.dumps(log_info, cls=NpEncoder))
        elif log_level == 'debug':logger.debug(json.dumps(log_info, cls=NpEncoder))
        elif log_level == 'warn': logger.warn(json.dumps(log_info, cls=NpEncoder))
        elif log_level == 'error':logger.error(json.dumps(log_info, cls=NpEncoder))
    
##########################################################################################################
#### UDF procedures for transformation data
##########################################################################################################

@udf(input_types=[DataTypes.STRING()], result_type=DataTypes.STRING())
def clean_html_from_tags(s: str = '0'):
    """
    Using library BeatifulSoap for cleansing text from tags
    """

    if s == '': return 'empty_data'
    
    for i in ['\t','\n','\r']: s = s.replace(i, ' ')
        
    try:
        return bs4.BeautifulSoup(s, "html").text
    except:
        return bs4.BeautifulSoup(s, "lxml").text


@udf(input_types=[DataTypes.BIGINT()], result_type=DataTypes.TIMESTAMP(3))
def from_timestamp(ts):
    """
    converting timestamp to datetime
    """
    
    try:
        return dt.datetime.fromtimestamp(ts)
    except:
        return dt.datetime(1970,1,1)