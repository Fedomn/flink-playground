import os

from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode, FsStateBackend
from pyflink.table import StreamTableEnvironment

hive_conf_dir = os.path.abspath('')


# export HADOOP_CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`
# ./bin/flink run --python ../pyflink/sql/flink_iceberg_versioned_table.py
def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000, CheckpointingMode.AT_LEAST_ONCE)
    env.set_state_backend(FsStateBackend(f"file:///tmp/flink_iceberg_versioned_table/checkpoints"))
    table_env = StreamTableEnvironment.create(env)

    # create mysql cdc table
    # CREATE TABLE `test` (
    #     `id` int NOT NULL AUTO_INCREMENT,
    #     `num` int DEFAULT NULL,
    # PRIMARY KEY (`id`)
    # ) ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

    table_env.execute_sql("""
    CREATE TABLE test (
        id INT,
        num INT,
        db_name STRING METADATA FROM 'database_name' VIRTUAL,
        table_name STRING METADATA  FROM 'table_name' VIRTUAL,
        operation_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
        PRIMARY KEY(id) NOT ENFORCED
    ) WITH (
        'connector' = 'mysql-cdc',
        'hostname' = 'localhost',
        'port' = '3306',
        'username' = 'root',
        'password' = '',
        'database-name' = 'test',
        'table-name' = 'test'
    );
    """)

    # https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/datastream/kafka/#fault-tolerance
    # delivery guarantee mode: exactly_once: However, this delays record visibility effectively until a checkpoint is
    # written, so adjust the checkpoint duration accordingly.
    table_env.execute_sql("""
    CREATE TABLE kafka (
        id INT,
        num INT,
        operation_ts TIMESTAMP(3),
        PRIMARY KEY (id) NOT ENFORCED,
        WATERMARK FOR operation_ts AS operation_ts
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'test',
        'properties.bootstrap.servers' = 'localhost:9092',
        'properties.group.id' = 'testGroup1',
        'scan.startup.mode' = 'latest-offset',
        'format' = 'debezium-json',
        'debezium-json.ignore-parse-errors' = 'false',
        'debezium-json.schema-include' = 'false'
    );
    """)
    table_env.execute_sql("INSERT INTO kafka SELECT id, num, operation_ts FROM test;")

    # table_env.execute_sql("select * from KafkaTable").print()

    table_env.execute_sql("""
    CREATE TABLE iceberg (
        id INT,
        num INT,
        operation_ts TIMESTAMP(3),
        PRIMARY KEY (`id`) NOT ENFORCED
    ) WITH (
        'connector' = 'iceberg',
        'catalog-name' = 'iceberg_catalog',
        'catalog-type' = 'hadoop',
        'warehouse' = 'file:///tmp/iceberg/warehouse',
        'format-version' = '2',
        'write.upsert.enabled'='true'
    );
    """)
    table_env.execute_sql("INSERT INTO iceberg SELECT * FROM kafka;")


if __name__ == '__main__':
    run()
