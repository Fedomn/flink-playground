import os

from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode, FsStateBackend, RuntimeExecutionMode
from pyflink.table import StreamTableEnvironment

hive_conf_dir = os.path.abspath('')


# export HADOOP_CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`
# ./bin/flink run --python ../pyflink/sql/flink_create_iceberg_table.py
def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000, CheckpointingMode.AT_LEAST_ONCE)
    env.set_state_backend(FsStateBackend(f"file:///tmp/flink_create_iceberg_table/checkpoints"))
    table_env = StreamTableEnvironment.create(env)

    # v2 with pk
    # table_env.execute_sql("""
    # CREATE TABLE iceberg (
    #     id INT,
    #     num INT,
    #     PRIMARY KEY (`id`) NOT ENFORCED
    # ) WITH (
    #     'connector' = 'iceberg',
    #     'catalog-name' = 'iceberg_catalog',
    #     'catalog-type' = 'hadoop',
    #     'warehouse' = 'file:///tmp/iceberg/warehouse',
    #     'format-version' = '2',
    #     'write.upsert.enabled'='true'
    # );
    # """)

    # v1 no pk
    table_env.execute_sql("""
    CREATE TABLE iceberg (
        id INT,
        num INT,
        op STRING,
        op_ts TIMESTAMP(3)
    ) WITH (
        'connector' = 'iceberg',
        'catalog-name' = 'iceberg_catalog',
        'catalog-type' = 'hadoop',
        'warehouse' = 'file:///tmp/iceberg/warehouse'
    );
    """)
    table_env.execute_sql("insert into iceberg select 1, 1, 'c', timestamp '2021-01-01 00:00:00'")
    # select * from iceberg;


if __name__ == '__main__':
    run()
