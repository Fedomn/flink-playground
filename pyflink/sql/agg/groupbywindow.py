from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env)

table_env.execute_sql("""
    CREATE TABLE server_logs ( 
    client_ip STRING,
    client_identity STRING, 
    userid STRING, 
    request_line STRING, 
    status_code STRING, 
    log_time AS PROCTIME()
) WITH (
  'connector' = 'faker', 
  'fields.client_ip.expression' = '#{Internet.publicIpV4Address}',
  'fields.client_identity.expression' =  '-',
  'fields.userid.expression' =  '-',
  'fields.log_time.expression' =  '#{date.past ''10'',''5'',''SECONDS''}',
  'fields.request_line.expression' = '#{regexify ''(GET|POST|PUT|PATCH){1}''} #{regexify ''(/search\.html|/login\.html|/prod\.html|cart\.html|/order\.html){1}''} #{regexify ''(HTTP/1\.1|HTTP/2|/HTTP/1\.0){1}''}',
  'fields.status_code.expression' = '#{regexify ''(200|201|204|400|401|403|301){1}''}'
);
    """)


def legacy_group_by_window():
    table_env.execute_sql("""
    SELECT
        COUNT(DISTINCT client_ip) AS ip_addresses,
        TUMBLE_PROCTIME(log_time, INTERVAL '10' SECONDS) AS window_interval,
        TUMBLE_START(log_time, INTERVAL '10' SECONDS) AS window_start,
        TUMBLE_END(log_time, INTERVAL '10' SECONDS) AS window_end
    FROM server_logs
    GROUP BY TUMBLE(log_time, INTERVAL '10' SECONDS);
    """).print()


def group_by_window_table_valued_function():
    """
    https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/queries/window-tvf/
    In streaming mode, the “window_time” field is a time attributes of the window.
    In batch mode, the “window_time” field is an attribute of type TIMESTAMP or TIMESTAMP_LTZ based on input time field type.
    """
    table_env.execute_sql("""
    SELECT 
        window_start,
        window_end,
        COUNT(DISTINCT client_ip) AS ip_addresses
    FROM TABLE(
        TUMBLE(TABLE server_logs, DESCRIPTOR(log_time), INTERVAL '10' SECONDS)
    )
    GROUP BY window_start, window_end
    """).print()


if __name__ == '__main__':
    # legacy_group_by_window()
    group_by_window_table_valued_function()
