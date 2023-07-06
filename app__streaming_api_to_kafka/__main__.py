from src.streamapi import *
from src.settings import configs

##################### init Kafka connector ---------------------------------

db_broker = DBkafka(
    topic = configs['KAFKA__TOPIC'],
    log_path = configs['LOG__PATH_LOCAL'],
    headers = {'version':'1.1'},
    tz = configs['timezone'],
    host = configs['KAFKA__HOST'], 
    port = configs['KAFKA__PORT'],
    value_serializer = lambda x: json.dumps(x).encode('utf-8'),
)

#################### init Stream processing instance ----------------------

apistream = streamAPI(
    url = configs['URL_STREAM'],
    log_path = configs['LOG__PATH_LOCAL'],
    db_broker = db_broker,
    pattern_msg = re.compile('(?P<name>[^:]*):?( ?(?P<value>.*))?'),
)

###########################################################################

if __name__ == "__main__":
    
    apistream.start_streaming()