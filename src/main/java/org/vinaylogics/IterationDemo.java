package org.vinaylogics;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class IterationDemo {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<Long, Integer>> dataStream = env.generateSequence(0,5)
                .map((MapFunction<Long, Tuple2<Long, Integer>>) value -> new Tuple2<>(value, 0))
                .returns(new TupleTypeInfo<>(Types.LONG, Types.INT));

        // Prepare stream for iteration
        IterativeStream<Tuple2<Long, Integer>> iteration = dataStream.iterate(5000);

        // Define iteration
        DataStream<Tuple2<Long, Integer>> plusOne = iteration.map(
                (MapFunction<Tuple2<Long, Integer>, Tuple2<Long, Integer>>) value -> {
            if(value.f0 == 10)
                return value;
            else
                return new Tuple2<>(value.f0+1, value.f1 +1);
        }).returns(new TupleTypeInfo<>(Types.LONG, Types.INT)); // plusone 1,1 2,1 3,1 4,1 5,1 6,1

        // part of stream used in next iteration
        DataStream<Tuple2<Long, Integer>> notEqualToTen = plusOne.filter(
                (FilterFunction<Tuple2<Long, Integer>>) value -> value.f0 != 10).returns(new TupleTypeInfo<>(Types.LONG, Types.INT));

        // feedback to next iteration
        iteration.closeWith(notEqualToTen);

        DataStream<Tuple2<Long, Integer>> equalToTen = plusOne.filter(
                (FilterFunction<Tuple2<Long, Integer>>) value -> value.f0 == 10).returns(new TupleTypeInfo<>(Types.LONG, Types.INT));

        equalToTen.writeAsText("/home/vinay/ten");

        env.execute("Iteration Demo");
    }
}
