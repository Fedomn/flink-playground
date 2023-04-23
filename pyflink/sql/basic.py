import os

from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode, \
    FsStateBackend
from pyflink.table import EnvironmentSettings, TableEnvironment, StreamTableEnvironment


def datagen_for_local_testing():
    """
    datagen connector: https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/connectors/table/datagen/
    """
    env = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env)
    table_env.execute_sql("""
    CREATE TABLE orders (
    order_uid  BIGINT,
    product_id BIGINT,
    price      DECIMAL(32, 2),
    order_time TIMESTAMP(3)
) WITH (
    'connector' = 'datagen'
);
    """)

    orders = table_env.from_path("orders")
    orders.execute().print()


def create_insert_temporary_table():
    """
    flink-faker: https://flink-packages.org/packages/flink-faker
    """
    env = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env)
    table_env.execute_sql("""
CREATE TEMPORARY TABLE server_logs ( 
    client_ip STRING,
    client_identity STRING, 
    userid STRING, 
    log_time TIMESTAMP(3),
    request_line STRING, 
    status_code STRING, 
    size INT
) WITH (
  'connector' = 'faker', 
  'fields.client_ip.expression' = '#{Internet.publicIpV4Address}',
  'fields.client_identity.expression' =  '-',
  'fields.userid.expression' =  '-',
  'fields.log_time.expression' =  '#{date.past ''15'',''5'',''SECONDS''}',
  'fields.request_line.expression' = '#{regexify ''(GET|POST|PUT|PATCH){1}''} #{regexify ''(/search\.html|/login\.html|/prod\.html|cart\.html|/order\.html){1}''} #{regexify ''(HTTP/1\.1|HTTP/2|/HTTP/1\.0){1}''}',
  'fields.status_code.expression' = '#{regexify ''(200|201|204|400|401|403|301){1}''}',
  'fields.size.expression' = '#{number.numberBetween ''100'',''10000000''}'
);
    """)
    table_env.execute_sql("""
CREATE TEMPORARY TABLE client_errors (
  log_time TIMESTAMP(3),
  request_line STRING,
  status_code STRING,
  size INT
)
WITH (
  'connector' = 'blackhole'
);    
    """)
    table_env.execute_sql("""
INSERT INTO client_errors
SELECT 
  log_time,
  request_line,
  status_code,
  size
FROM server_logs
WHERE status_code SIMILAR TO '4[0-9][0-9]';    
    """)


def order_by_unbounded_tables():
    """
    The event time attribute is defined using a WATERMARK statement in CREATE table DDL.
    """
    env = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env)
    table_env.execute_sql("""
CREATE TEMPORARY TABLE server_logs (
    client_ip STRING,
    client_identity STRING, 
    userid STRING, 
    user_agent STRING,
    log_time TIMESTAMP(3),
    request_line STRING, 
    status_code STRING, 
    size INT, 
    WATERMARK FOR log_time AS log_time - INTERVAL '15' SECONDS
) WITH (
  'connector' = 'faker', 
  'fields.client_ip.expression' = '#{Internet.publicIpV4Address}',
  'fields.client_identity.expression' =  '-',
  'fields.userid.expression' =  '-',
  'fields.user_agent.expression' = '#{Internet.userAgent}',
  'fields.log_time.expression' =  '#{date.past ''15'',''5'',''SECONDS''}',
  'fields.request_line.expression' = '#{regexify ''(GET|POST|PUT|PATCH){1}''} #{regexify ''(/search\.html|/login\.html|/prod\.html|cart\.html|/order\.html){1}''} #{regexify ''(HTTP/1\.1|HTTP/2|/HTTP/1\.0){1}''}',
  'fields.status_code.expression' = '#{regexify ''(200|201|204|400|401|403|301){1}''}',
  'fields.size.expression' = '#{number.numberBetween ''100'',''10000000''}'
);    
    """)
    table_env.execute_sql("""
SELECT
  TUMBLE_ROWTIME(log_time, INTERVAL '1' MINUTE) AS window_time,
  REGEXP_EXTRACT(user_agent,'[^\/]+') AS browser,
  COUNT(*) AS cnt_browser
FROM server_logs
GROUP BY
  REGEXP_EXTRACT(user_agent,'[^\/]+'),
  TUMBLE(log_time, INTERVAL '1' MINUTE)
ORDER BY
  window_time,
  cnt_browser DESC;
    """).print()


def write_multi_sinks():
    """
    https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/connectors/table/kafka/
    https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/connectors/table/filesystem/
    需要启动kafka，可以通过consumer命令行观察输出情况

    解决hadoop依赖：
    https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/configuration/advanced/#hadoop-dependencies

    FileSink问题：https://stackoverflow.com/questions/65546792/how-does-the-file-system-connector-sink-work
    https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/connectors/datastream/filesystem/#file-sink
    """
    abspath = os.path.abspath('')  # pyflink/sql
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(65 * 1000, CheckpointingMode.AT_LEAST_ONCE)
    env.set_state_backend(FsStateBackend(f"file://{abspath}/checkpoints"))
    table_env = StreamTableEnvironment.create(env)
    # env = EnvironmentSettings.in_streaming_mode()
    # table_env = TableEnvironment.create(env)
    table_env.execute_sql("""
CREATE TEMPORARY TABLE server_logs ( 
    client_ip       STRING,
    client_identity STRING, 
    userid          STRING, 
    user_agent      STRING,
    log_time        TIMESTAMP(3),
    request_line    STRING, 
    status_code     STRING, 
    size            INT,
    WATERMARK FOR log_time AS log_time - INTERVAL '0' SECONDS
) WITH (
  'connector' = 'faker', 
  'fields.client_ip.expression' = '#{Internet.publicIpV4Address}',
  'fields.client_identity.expression' =  '-',
  'fields.userid.expression' =  '-',
  'fields.user_agent.expression' = '#{Internet.userAgent}',
  'fields.log_time.expression' =  '#{date.past ''65'',''5'',''SECONDS''}',
  'fields.request_line.expression' = '#{regexify ''(GET|POST|PUT|PATCH){1}''} #{regexify ''(/search\.html|/login\.html|/prod\.html|cart\.html|/order\.html){1}''} #{regexify ''(HTTP/1\.1|HTTP/2|/HTTP/1\.0){1}''}',
  'fields.status_code.expression' = '#{regexify ''(200|201|204|400|401|403|301){1}''}',
  'fields.size.expression' = '#{number.numberBetween ''100'',''10000000''}'
);    
    """)
    table_env.execute_sql("""
CREATE TEMPORARY TABLE realtime_aggregations (
  `browser`     STRING,
  `status_code` STRING,
  `end_time`    TIMESTAMP(3),
  `requests`    BIGINT NOT NULL
) WITH (
  'connector' = 'kafka',
  'topic' = 'browser-status-codes', 
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'browser-countds',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json' 
);    
    """)
    table_env.execute_sql(f"""
CREATE TEMPORARY TABLE offline_datawarehouse (
    `browser`     STRING,
    `status_code` STRING,
    `dt`          STRING,
    `hour`        STRING,
    `minute`      STRING,
    `requests`    BIGINT NOT NULL
) PARTITIONED BY (`dt`, `hour`, `minute`) WITH (
  'connector' = 'filesystem',
  'path' = 'file:///{abspath}/offline_datawarehouse',
  'sink.partition-commit.trigger' = 'partition-time', 
  'sink.partition-commit.watermark-time-zone' = 'Asia/Shanghai',
  'sink.partition-commit.policy.kind' = 'success-file',
  'sink.partition-commit.delay' = '0s',
  'sink.rolling-policy.check-interval' = '5s',
  'sink.rolling-policy.rollover-interval' = '65s',
  'sink.rolling-policy.inactivity-interval' = '80s',
  'format' = 'json' 
);    
    """)
    table_env.execute_sql("""
CREATE TEMPORARY VIEW browsers AS  
SELECT 
  REGEXP_EXTRACT(user_agent,'[^\/]+') AS browser,
  status_code,
  log_time
FROM server_logs;    
    """)
    table_env.execute_sql("""
INSERT INTO realtime_aggregations
SELECT
    browser,
    status_code,
    TUMBLE_ROWTIME(log_time, INTERVAL '10' SECONDS) AS end_time,
    COUNT(*) requests
FROM browsers
GROUP BY 
    browser,
    status_code,
    TUMBLE(log_time, INTERVAL '10' SECONDS);    
    """)
    table_env.execute_sql("""
INSERT INTO offline_datawarehouse
SELECT
    browser,
    status_code,
    DATE_FORMAT(TUMBLE_ROWTIME(log_time, INTERVAL '1' MINUTE), 'yyyy-MM-dd') AS `dt`,
    DATE_FORMAT(TUMBLE_ROWTIME(log_time, INTERVAL '1' MINUTE), 'HH') AS `hour`,
    DATE_FORMAT(TUMBLE_ROWTIME(log_time, INTERVAL '1' MINUTE), 'mm') AS `minute`,
    COUNT(*) requests
FROM browsers
GROUP BY 
    browser,
    status_code,
    TUMBLE(log_time, INTERVAL '1' MINUTE);    
    """)
    table_env.execute_sql("select * from realtime_aggregations").print()


if __name__ == '__main__':
    # datagen_for_local_testing()
    # create_insert_temporary_table()
    # order_by_unbounded_tables()
    write_multi_sinks()
