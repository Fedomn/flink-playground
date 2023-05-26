import os

from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode, FsStateBackend
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, TableEnvironment

hive_conf_dir = os.path.abspath('')


# export HADOOP_CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`
# ./bin/flink run --python ../pyflink/sql/iceberg_hive_catalog.py
def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(65 * 1000, CheckpointingMode.AT_LEAST_ONCE)
    env.set_state_backend(FsStateBackend(f"hdfs:///iceberg_catalog/checkpoints"))
    table_env = StreamTableEnvironment.create(env)

    # create iceberg hive catalog and table
    table_env.execute_sql(f"""
CREATE CATALOG iceberg_catalog WITH (
    'type'='iceberg',
    'catalog-type'='hive',
    'uri'='thrift://localhost:9083',
    'hive-conf-dir'='{hive_conf_dir}'
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
    table_env.execute_sql("drop table if exists iceberg_catalog.db.offline_datawarehouse")
    table_env.execute_sql(f"""
CREATE TABLE IF NOT EXISTS iceberg_catalog.db.offline_datawarehouse (
    `browser`     STRING,
    `status_code` STRING,
    `dt`          STRING,
    `hour`        STRING,
    `minute`      STRING,
    `requests`    BIGINT NOT NULL
) PARTITIONED BY (`dt`);
    """)
    table_env.execute_sql("""
CREATE TEMPORARY VIEW browsers AS
SELECT 
  REGEXP_EXTRACT(user_agent,'[^\/]+') AS browser,
  status_code,
  log_time
FROM server_logs;    
    """)
    # 它的时间只能比checkpoint的时间大，不能比checkpoint的时间小（如果小，则写文件的时间间隔还是checkpoint的时间）
    table_env.execute_sql("""
INSERT INTO iceberg_catalog.db.offline_datawarehouse
SELECT
    browser,
    status_code,
    DATE_FORMAT(TUMBLE_ROWTIME(log_time, INTERVAL '5' SECOND), 'yyyy-MM-dd') AS `dt`,
    DATE_FORMAT(TUMBLE_ROWTIME(log_time, INTERVAL '5' SECOND), 'HH') AS `hour`,
    DATE_FORMAT(TUMBLE_ROWTIME(log_time, INTERVAL '5' SECOND), 'mm') AS `minute`,
    COUNT(*) requests
FROM browsers
GROUP BY
    browser,
    status_code,
    TUMBLE(log_time, INTERVAL '5' SECOND);
    """)
    # 通过jdbc来达到实时的sink
    table_env.execute_sql("""
    CREATE TABLE test (
        `id` int,
        `num` int,
        primary key (id) not enforced
    ) WITH (
      'connector' = 'jdbc',
      'url'='jdbc:postgresql://127.0.0.1:5432/test',
      'username' = 'postgres' ,
      'password' = '', 
      'table-name'='test'
    );
        """)
    # -- Pg Table Definition
    # CREATE TABLE "public"."test" (
    #     "num" int8 NOT NULL,
    #     "id" int8 NOT NULL,
    #     PRIMARY KEY ("id")
    # );
    table_env.execute_sql("""
    insert into test SELECT 1 as id, cast(COUNT(*) as int) as num FROM browsers;
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
