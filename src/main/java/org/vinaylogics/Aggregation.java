package org.vinaylogics;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Aggregation {
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> data = env.readTextFile("/home/vinay/avg1");

        DataStream<Tuple4<String, String, String, Integer>> mapped = data.map(new Splitter());

        mapped.keyBy(0).sum(3).writeAsText("/home/vinay/out1");
        mapped.keyBy(0).min(3).writeAsText("/home/vinay/out2");
        mapped.keyBy(0).minBy(3).writeAsText("/home/vinay/out3");
        mapped.keyBy(0).max(3).writeAsText("/home/vinay/out4");
        mapped.keyBy(0).maxBy(3).writeAsText("/home/vinay/out5");

        // execute program
        env.execute("Agregation");
    }

    public static final class Splitter implements MapFunction<String, Tuple4<String, String, String, Integer>> {

        @Override
        public Tuple4<String, String, String, Integer> map(String value) throws Exception {
            var words = value.split(",");
            return new Tuple4<>(words[1], words[2], words[3], Integer.parseInt(words[4]));
        }
    }
}
