package org.playground.flink.datastream;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;

import org.playground.flink.datastream.CustomDebeziumJsonDeserializationSchema.MetadataConverter;


public enum ReadableMetadata {
  SCHEMA(
      "schema",
      DataTypes.STRING().nullable(),
      false,
      DataTypes.FIELD("schema", DataTypes.STRING()),
      new MetadataConverter() {
        private static final long serialVersionUID = 1L;

        @Override
        public Object convert(GenericRowData row, int pos) {
          return row.getString(pos);
        }
      }),

  OP(
      "op",
      DataTypes.STRING().nullable(),
      true,
      DataTypes.FIELD("op", DataTypes.STRING()),
      new MetadataConverter() {
        private static final long serialVersionUID = 1L;

        @Override
        public Object convert(GenericRowData row, int pos) {
          if (row.isNullAt(pos)) {
            return null;
          }
          return row.getString(pos);
        }
      }),

  INGESTION_TIMESTAMP(
      "ingestion-timestamp",
      DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).nullable(),
      true,
      DataTypes.FIELD("ts_ms", DataTypes.BIGINT()),
      new MetadataConverter() {
        private static final long serialVersionUID = 1L;

        @Override
        public Object convert(GenericRowData row, int pos) {
          if (row.isNullAt(pos)) {
            return null;
          }
          return TimestampData.fromEpochMillis(row.getLong(pos));
        }
      }),

  SOURCE_TIMESTAMP(
      "source.timestamp",
      DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).nullable(),
      true,
      DataTypes.FIELD("source", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())),
      new MetadataConverter() {
        private static final long serialVersionUID = 1L;

        @Override
        public Object convert(GenericRowData row, int pos) {
          final StringData timestamp =
              (StringData) readProperty(row, pos, StringData.fromString("ts_ms"));
          if (timestamp == null) {
            return null;
          }
          return TimestampData.fromEpochMillis(Long.parseLong(timestamp.toString()));
        }
      }),

  SOURCE_DATABASE(
      "source.database",
      DataTypes.STRING().nullable(),
      true,
      DataTypes.FIELD("source", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())),
      new MetadataConverter() {
        private static final long serialVersionUID = 1L;

        @Override
        public Object convert(GenericRowData row, int pos) {
          return readProperty(row, pos, StringData.fromString("db"));
        }
      }),

  SOURCE_SCHEMA(
      "source.schema",
      DataTypes.STRING().nullable(),
      true,
      DataTypes.FIELD("source", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())),
      new MetadataConverter() {
        private static final long serialVersionUID = 1L;

        @Override
        public Object convert(GenericRowData row, int pos) {
          return readProperty(row, pos, StringData.fromString("schema"));
        }
      }),

  SOURCE_TABLE(
      "source.table",
      DataTypes.STRING().nullable(),
      true,
      DataTypes.FIELD("source", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())),
      new MetadataConverter() {
        private static final long serialVersionUID = 1L;

        @Override
        public Object convert(GenericRowData row, int pos) {
          return readProperty(row, pos, StringData.fromString("table"));
        }
      }),

  SOURCE_PROPERTIES(
      "source.properties",
      // key and value of the map are nullable to make handling easier in queries
      DataTypes.MAP(DataTypes.STRING().nullable(), DataTypes.STRING().nullable())
          .nullable(),
      true,
      DataTypes.FIELD("source", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())),
      new MetadataConverter() {
        private static final long serialVersionUID = 1L;

        @Override
        public Object convert(GenericRowData row, int pos) {
          return row.getMap(pos);
        }
      });

  final String key;

  final DataType dataType;

  final boolean isJsonPayload;

  final DataTypes.Field requiredJsonField;

  final MetadataConverter converter;

  ReadableMetadata(
      String key,
      DataType dataType,
      boolean isJsonPayload,
      DataTypes.Field requiredJsonField,
      MetadataConverter converter) {
    this.key = key;
    this.dataType = dataType;
    this.isJsonPayload = isJsonPayload;
    this.requiredJsonField = requiredJsonField;
    this.converter = converter;
  }

  private static Object readProperty(GenericRowData row, int pos, StringData key) {
    final GenericMapData map = (GenericMapData) row.getMap(pos);
    if (map == null) {
      return null;
    }
    return map.get(key);
  }
}