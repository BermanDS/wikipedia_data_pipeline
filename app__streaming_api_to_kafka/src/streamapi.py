from .utils import *


class streamAPI:

    def __init__(self, 
                headers: dict = {},
                url: str = None,
                domain: str = 'wikipedia.org',
                log_path: str = os.getcwd(),
                log_level: str = 'error',
                pattern_msg: object = None,
                db_broker: object = None):
        
        self.headers = headers
        self.url = url
        self.method = 'GET'
        self.location = 'streaming_API'
        self.pattern_msg = pattern_msg
        self.response = None
        self.db_broker = db_broker
        self.log_path = log_path
        self.result = {}
        self.retries = [0, 1, 1, 2, 3, 5, 8, 13, 21, 34]
        self.domain = domain
        self.counter = 0
        self.df = pd.DataFrame()
        self.log_level = log_level
        self.log_levels = ["debug", "info", "warn", "error"]
        self._logger_init()

    
    def _logger_init(self):

        reload(logging)
        
        logging.basicConfig(
            filename = os.path.join(self.log_path, f'{self.location}.log'),
            level = logging.INFO if self.log_level == 'info' \
                    else logging.ERROR if self.log_level == 'error' \
                    else logging.WARN if self.log_level == 'warn' \
                    else logging.DEBUG,
        )
        self.logger = logging.getLogger('API_STREAM')

    
    def log(self, tag: str = 'streaming', log_level: str = 'info', message: str = '', data: dict = {}) -> None:

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
    

    def start_streaming(self):
        """
        main procedure of initialization streaming parsing
        """
        
        if self.db_broker:
            self.log('start process', "info", f"Streaming from {self.url}...")
            load = True
        else:
            self.log('kafka-definition', 'error', 'Kafka instance did not define')
            load = False
        
        while load:
            count_retries = len(self.retries)
            
            for attempt, duration in enumerate(self.retries):
                
                attempt_time = int(time())

                try:
                    self.log('initialization',"debug", f"Starting the run loop... {attempt}/{count_retries}")
                    self.go_loop()
                except asyncio.TimeoutError:
                    self.log('break connection',"warn",\
                             f"Timed out attempting to stream from '{self.url}'. Retrying in {duration} seconds... ({attempt}/{count_retries})")
                    sleep(duration)
                    error_time = int(time())
                    if ((error_time - attempt_time) > max(self.retries) + 1):
                        self.log('reset option',"debug", f"Resetting retry counter after {error_time - attempt_time} seconds.")
                        break
                    else:
                        self.log('retry',"debug", f"Retrying after {error_time - attempt_time} seconds...")
                except BaseException as error:
                    self.log('retry',"error", \
                            f"Error attempting to stream from '{self.url}'. Retrying in {duration} seconds... ({attempt}/{count_retries}):{error}")
                    sleep(duration)
                    error_time = int(time())
                    if ((error_time - attempt_time) > max(self.retries) + 1):
                        self.log('reset option',"debug", f"Resetting retry counter after {error_time - attempt_time} seconds.")
                        break
                    else:
                        self.log("debug", "debug", f"Retrying after {error_time - attempt_time} seconds...")
                except:
                    self.log('other error',"error", \
                            f"Unexpected error attempting to stream from '{self.url}'. Retrying in {duration} seconds... ({attempt}/{count_retries})")
                
            else:
                self.log('fatal error',"error", f"Failed permanently after {count_retries} attempts to stream from '{self.url}'.")
                break
    

    def go_loop(self):
        """
        running streaming kernel
        """

        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.streaming())


    async def streaming(self):
        async for event in self.aiosseclient():
            
            self.publish(event)
    
            
    async def aiosseclient(self, last_id=None, **kwargs):
            
        
        kwargs['headers'] = self.headers
            
        # The SSE spec requires making requests with Cache-Control: nocache
        kwargs['headers']['Cache-Control'] = 'no-cache'

        # The 'Accept' header is not required, but explicit > implicit
        kwargs['headers']['Accept'] = 'text/event-stream'

        if last_id:
            kwargs['headers']['Last-Event-ID'] = last_id

        async with aiohttp.ClientSession() as session:
            response = await session.get(self.url, **kwargs)
            lines = []
            async for line in response.content:
                
                line = line.decode('utf8')

                if (line == '\n' or line == '\r' or line == '\r\n'):
                    if lines[0] == ':ok\n':
                        lines = []
                        continue
                    yield self.parse_wiki_resentchange(''.join(lines))
                    lines = []
                else:
                    lines.append(line)
                
    
    def parse_wiki_resentchange(self, raw: str = ''):
        """
        parsing general info about frequently using pages
        and users for common analisys
        Approach 1
        """

        self.main_columns = ['title','timestamp','user','bot','parsedcomment','type']

        for line in raw.splitlines():
            
            m = self.pattern_msg.match(line)

            if m is None:
                continue

            name = m.group('name')
            if name == '':
                # line began with a ":", so is a comment.  Ignore
                continue
            
            value = m.group('value')

            if name == 'data':
                # processing necessary values
                try:
                    temp_dc = json.loads(value)
                    self.domain = json_extract(temp_dc, 'domain')
                    self.domain = self.domain[0] if self.domain != [] else ''
                except:
                    continue
            
                return {x.replace(x, f"{x}_"):temp_dc[x] if x in temp_dc.keys() else None for x in self.main_columns}

        return {}
    
        
    def publish(self, data: dict  = {}):
        """
        procedure of publishing data to Kafka
        by using connector db_broker
        """

        if data != {}:
            data["domain_"] = self.domain

            self.db_broker.publish_message(None, data)