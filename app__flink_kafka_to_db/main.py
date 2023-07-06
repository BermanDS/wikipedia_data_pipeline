from src.utils import *
from src.settings import configs


## define JAR packages
jarlibs = [os.path.join(os.getcwd(), configs['JARS__DATA'],x) for x in os.listdir(os.path.join(os.getcwd(), configs['JARS__DATA']))]

## define scripts
scripts = {x.split('.')[0]: open(os.path.join(os.getcwd(), configs['SCRIPTS__DATA'],x), 'r').read() for x in os.listdir(os.path.join(os.getcwd(), configs['SCRIPTS__DATA']))}

## logging

logger = logger_init(location = 'flink_app', log_path = configs['LOG__PATH'])

if __name__ == "__main__":
    
    # Create streaming environment
    env = StreamExecutionEnvironment.get_execution_environment()

    settings = EnvironmentSettings.new_instance()\
                                  .in_streaming_mode()\
                                  .use_blink_planner()\
                                  .build()
    
    # Create table environment
    tbl_env = StreamTableEnvironment.create(stream_execution_environment = env,
                                            environment_settings = settings)
    
    try:
        # add kafka and jdbc connectors dependency
        tbl_env.get_config()\
                .get_configuration()\
                .set_string("pipeline.jars", ';'.join([f"file://{x}" for x in jarlibs]))
    except Exception as err:
        log(logger, 'initialization main dependecies', 'error', str(err))
    
    ##register udf
    tbl_env.register_function("from_timestamp", from_timestamp)
    tbl_env.register_function("clean_html_from_tags", clean_html_from_tags)

    try:
        # register source and target tables
        tbl_env.sql_update(scripts['source_definition'].format(**configs))
        tbl_env.sql_update(scripts['target_definition'].format(**configs))
    except Exception as err:
        log(logger, 'registration of sinks', 'error', str(err))
    
    try:
        ## defining transformation
        tbl_env.from_path(configs['table_source'])\
               .select(scripts['transformation'])\
               .insert_into(configs['table_target'])
    except Exception as err:
        log(logger, 'transformation', 'error', str(err))
    
    try:
        tbl_env.execute("pyFlink_parse_kafka")
    except Exception as err:
        log(logger, 'execution', 'error', str(err))

