import os

from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode, FsStateBackend
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, TableEnvironment

hive_conf_dir = os.path.abspath('')


def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(65 * 1000, CheckpointingMode.AT_LEAST_ONCE)
    env.set_state_backend(FsStateBackend(f"hdfs:///iceberg_catalog/checkpoints"))
    table_env = StreamTableEnvironment.create(env)

    # create hive catalog and table
    table_env.execute_sql(f"""
CREATE CATALOG myhive WITH (
    'type' = 'hive',
    'hive-conf-dir'='{hive_conf_dir}'
);
""")
    table_env.execute_sql("""
    create table if not exists myhive.db.test (
        id int
    );
""")
    table_env.execute_sql("""
    insert into myhive.db.test values (1);
    """)

    # create iceberg hive catalog and table
    table_env.execute_sql(f"""
CREATE CATALOG iceberg_catalog WITH (
    'type'='iceberg',
    'catalog-type'='hive',
    'uri'='thrift://localhost:9083',
    'hive-conf-dir'='{hive_conf_dir}',
);""")
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

    table_env.execute_sql(f"""
CREATE TABLE IF NOT EXISTS iceberg_catalog.db.offline_datawarehouse (
    `browser`     STRING,
    `status_code` STRING,
    `dt`          STRING,
    `hour`        STRING,
    `minute`      STRING,
    `requests`    BIGINT NOT NULL
) PARTITIONED BY (`dt`, `hour`, `minute`) WITH (
  'sink.partition-commit.trigger' = 'partition-time', 
  'sink.partition-commit.watermark-time-zone' = 'Asia/Shanghai',
  'sink.partition-commit.policy.kind' = 'success-file',
  'sink.partition-commit.delay' = '0s',
  'sink.rolling-policy.check-interval' = '5s',
  'sink.rolling-policy.rollover-interval' = '65s',
  'sink.rolling-policy.inactivity-interval' = '80s'
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


def read():
    env = EnvironmentSettings.in_batch_mode()
    table_env = TableEnvironment.create(env)

    table_env.execute_sql(f"""
    CREATE CATALOG iceberg_catalog WITH (
        'type'='iceberg',
        'catalog-type'='hive',
        'uri'='thrift://localhost:9083',
        'hive-conf-dir'='{hive_conf_dir}'
    );    
    """)
    table_env.execute_sql(f"""
    CREATE CATALOG myhive WITH (
        'type' = 'hive',
        'hive-conf-dir'='{hive_conf_dir}'
    );
    """)

    table_env.execute_sql("use catalog myhive").print()

    table_env.execute_sql("show databases").print()
    table_env.execute_sql("show tables").print()
    table_env.execute_sql("insert into test values (3)").print()
    table_env.execute_sql("select * from test limit 10").print()
    table_env.execute_sql("select * from iceberg_catalog.db.offline_datawarehouse limit 10").print()


if __name__ == '__main__':
    run()
