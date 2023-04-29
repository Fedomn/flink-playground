package org.playground.flink.datastream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/** nc -l 9999 */
public class WindowWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, Integer>> stream =
                env.socketTextStream("localhost", 9999)
                        .flatMap(new Splitter())
                        .keyBy(v -> v.f0)
                        .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                        .sum(1);
        stream.print();
        env.execute("Window WordCount");
    }

    private static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(final String value, final Collector<Tuple2<String, Integer>> out) {
            for (final String word : value.split(" ")) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
