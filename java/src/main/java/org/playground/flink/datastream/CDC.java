package org.playground.flink.datastream;

import com.google.common.collect.Lists;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

/**
* CDC -> Kafka Sink -> Kafka Source -> Iceberg Sink。
* 其中，Kafka Source需要自定义 CustomDebeziumJsonDeserializationSchema 来实现自定义的 row data。
* 但是，需要事先定义 iceberg table 的 column names, types 传递给 CustomDebeziumJsonDeserializationSchema。
*/
public class CDC {

  public static void test_py4j(StreamExecutionEnvironment env) throws Exception {
    MySqlSource<String> mysqlSource =
        MySqlSource.<String>builder()
            .hostname("localhost")
            .port(3306)
            .databaseList("test") // set captured database
            .tableList("test.test") // set captured table
            .username("root")
            .password("")
            .deserializer(new JsonDebeziumDeserializationSchema(false)) // converts SourceRecord to JSON String
            .includeSchemaChanges(true)
            .build();

    env.enableCheckpointing(10 * 1000);

    DataStream<String> cdcSource =
        env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "cdctest").setParallelism(1);
    cdcSource.print();
    env.execute("MySql CDC");
  }

  public static void main(String[] args) throws Exception {
    MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
        .hostname("localhost")
        .port(3306)
        .databaseList("test") // set captured database
        .tableList("test.test") // set captured table
        .username("root")
        .password("")
        .deserializer(new JsonDebeziumDeserializationSchema(false)) // converts SourceRecord to JSON String
        .build();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // enable checkpoint
    env.enableCheckpointing(10*1000);

    DataStream<String> cdcSource = env
        .fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "cdctest")
        .setParallelism(1);
    // source.process(new MyProcessFunction());

    var producerProperties = new Properties();
    producerProperties.setProperty("transaction.timeout.ms", "60000");

    KafkaRecordSerializationSchema<String> serializer = KafkaRecordSerializationSchema.builder()
        .setTopic("test")
        .setValueSerializationSchema(new SimpleStringSchema())
        .build();
    KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
        .setBootstrapServers("localhost:9092")
        .setRecordSerializer(serializer)
        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
        .setKafkaProducerConfig(producerProperties)
        .build();

    cdcSource.sinkTo(kafkaSink);

    // 定义 CDC 数据的物理数据类型
    // alternative:
    // read json(in jar) and generate types and names
    // read iceberg table schema and generate types and names
    LogicalType[] types = {
        new IntType(), new IntType()
    };
    String[] names = {
        "id", "num"
    };
    DataType physicalDataType = TypeConversions.fromLogicalToDataType(
        RowType.of(
            types,
            names
        )
    );


    // 定义反序列化后的数据类型
    TypeInformation<RowData> producedTypeInfo = TypeInformation.of(RowData.class);
    CustomDebeziumJsonDeserializationSchema deserializationSchema = new CustomDebeziumJsonDeserializationSchema(
        physicalDataType,
        Lists.newArrayList(
            ReadableMetadata.OP,
            ReadableMetadata.INGESTION_TIMESTAMP
        ),
        producedTypeInfo,
        false,
        false,
        TimestampFormat.ISO_8601
    );

    // kafka source
    KafkaSource<RowData> kafkaSource = KafkaSource.<RowData>builder()
        .setBootstrapServers("localhost:9092")
        .setTopics("test")
        .setGroupId("my-group1")
        .setStartingOffsets(OffsetsInitializer.latest())
        .setValueOnlyDeserializer(deserializationSchema)
        .build();
    DataStream<RowData> kafkaSource2 = env
        .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource")
        .setParallelism(1);
    kafkaSource2.print();

    // iceberg sink
    var hadoopConf = new Configuration();
    // CatalogLoader.hive("iceberg", "/")
    TableLoader tableLoader = TableLoader.fromHadoopTable("file:///tmp/iceberg/warehouse/default_database/iceberg", hadoopConf);
    FlinkSink.forRowData(kafkaSource2)
        .tableLoader(tableLoader)
        // .upsert(true)
        .append();


    // stream -> table (column type)
    env.execute("MySQL CDC");
  }

  private static class MyProcessFunction extends ProcessFunction<String, Row> {
    @Override public void processElement(final String s, final ProcessFunction<String, Row>.Context context,
        final Collector<Row> collector) throws Exception {
      System.out.println(s);
      System.out.println(context.timestamp());
      Row row = Row.ofKind(RowKind.INSERT, s);
      collector.collect(row);
    }
  }
}
