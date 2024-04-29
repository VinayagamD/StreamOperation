package org.vinaylogics.state.managedkeystate.reduced;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class StateDemo {

    public static void main(String[] args) throws Exception
    {
        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> data = env.socketTextStream("localhost", 9090);

        DataStream<Long> sum = data.map((MapFunction<String, Tuple2<Long,String>>) s -> {
            String[] words = s.split(",");
            return new Tuple2<>(Long.parseLong(words[0]), words[1]);
        }).returns(new TupleTypeInfo<>(Types.LONG, Types.STRING))
                .keyBy(f-> f.f0)
                .flatMap(new StatefulMap());
      /*  sum.addSink(StreamingFileSink
                .forRowFormat(new Path("/home/vinay/state2"), new SimpleStringEncoder< Long >("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder().build())
                .build());*/
        DataStream<Row> dataRow = sum.map((MapFunction<Long, Row>) value -> {
            Row row = new Row(1);
            row.setField(0, value);
            return row;
        });

        FileSink<Row> dataSink = FileSink.
                forRowFormat(new Path("home/vinay/state2"),
                        (Encoder<Row>) (row, outputStream)  -> {
                            outputStream.write('(');
                            outputStream.write(row.getField(0).toString().getBytes());
                            outputStream.write(')');
                            outputStream.write('\n');
                        }
                ).build();
        dataRow.sinkTo(dataSink);
        // execute program
        env.execute("State");
    }

    public static class StatefulMap extends RichFlatMapFunction<Tuple2<Long, String>, Long>
    {
        private transient ValueState<Long> count;            // 2
        private transient ReducingState<Long> sum;          //  4

        public void flatMap(Tuple2<Long, String> input, Collector<Long> out)throws Exception
        {
            Long currValue = Long.parseLong(input.f1);
            Long currCount = count.value();             //  4

            currCount += 1;

            count.update(currCount);
            sum.add(currValue);

            if (currCount >= 10)
            {

                /* emit sum of last 10 elements */
                out.collect(sum.get());
                /* clear value */
                count.clear();
                sum.clear();
            }
        }

        public void open(Configuration conf)
        {

            ValueStateDescriptor<Long> descriptor2 = new ValueStateDescriptor<>("count", TypeInformation.of(new TypeHint<>() {
            }),0L);
            count = getRuntimeContext().getState(descriptor2);
            ReducingStateDescriptor<Long> sumDesc = new ReducingStateDescriptor<>("reducing sum", new SumReduce(), Long.class) ;
            sum = getRuntimeContext().getReducingState(sumDesc);
        }

        public class SumReduce implements ReduceFunction<Long> {

            @Override
            public Long reduce(Long cumulativeSum, Long currentValue) throws Exception {
                return cumulativeSum+currentValue;
            }
        }

    }

}
