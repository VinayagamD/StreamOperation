package org.vinaylogics.state.operators.checkpointing;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class StateDemo {

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // start a checkpoint every 1000 ms
        env.enableCheckpointing(1000);

        // to set minimum progress time to happen between checkpoints
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        // checkpoints have to complete within 10000 ms, or are discarded
        env.getCheckpointConfig().setCheckpointTimeout(10000);

        // set mode to exactly-once (this is the default)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); // AT_LEAST_ONCE

        //allow only one checkpoint to be in progress at the same time
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // enable externalized checkpoints which are retained after job cancellation
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION); // DELETE_ON_CANCELLATION

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,10));

        DataStream<String> data = env.socketTextStream("localhost", 9090);

        DataStream<Long> sum = data.map((MapFunction<String, Tuple2<Long, String>>) value -> {
            String[] words = value.split(",");
            return new Tuple2<>(Long.parseLong(words[0]), words[1]);
        }).returns(new TupleTypeInfo<>(Types.LONG,Types.STRING))
                .keyBy(f-> f.f0)
                .flatMap(new StatefulMap());

        DataStream<Row>  dataRow = sum.map((MapFunction<Long, Row>) value -> {
            Row row = new Row(1);
            row.setField(0, value);
            return row;
        });

        FileSink<Row> fileSink = FileSink.forRowFormat(
                new Path("/home/vinay/state2"),
                (Encoder<Row>) (row, outputStream) -> {
                    outputStream.write('(');
                    outputStream.write(row.getField(0).toString().getBytes());
                    outputStream.write(')');
                    outputStream.write('\n');
                }
        ).build();

        dataRow.sinkTo(fileSink);

        // execute program
        env.execute("State");


    }

    public static class StatefulMap extends RichFlatMapFunction<Tuple2<Long, String>, Long> {
        private transient ValueState<Long> sum;
        private transient ValueState<Long> count;


        @Override
        public void flatMap(Tuple2<Long, String> input, Collector<Long> out) throws Exception {
            Long currCount = count.value();
            Long currSum = sum.value();

            currCount+= 1;
            currSum = currSum + Long.parseLong(input.f1);

            count.update(currCount);
            sum.update(currSum);

            if(currCount >= 10) {
                /*emits sum of last 10 elements */
                out.collect(sum.value());
                /*clear value*/
                count.clear();
                sum.clear();
            }
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>("sum", TypeInformation.of(new TypeHint<>() {}), 0L);
            sum = getRuntimeContext().getState(descriptor);

            ValueStateDescriptor<Long> descriptor2 =  new ValueStateDescriptor<>("count", TypeInformation.of(new TypeHint<>() {}), 0L);
            count = getRuntimeContext().getState(descriptor2);
        }
    }
}
