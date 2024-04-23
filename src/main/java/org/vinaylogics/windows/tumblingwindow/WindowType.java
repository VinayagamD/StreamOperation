package org.vinaylogics.windows.tumblingwindow;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;

import java.sql.Timestamp;

public class WindowType {

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> data = env.socketTextStream("localhost", 9090);
        DataStream<Tuple2<Long, String>> sum = data.map((MapFunction<String, Tuple2<Long, String>>) s -> {
            String[] words = s.split(",");
            return new Tuple2<>(Long.parseLong(words[0]), words[1]);
        }).returns(new TupleTypeInfo<>(Types.LONG,Types.STRING))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<>() {
            @Override
            public long extractAscendingTimestamp(Tuple2<Long, String> t) {
                return t.f0;
            }
        }).windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce((ReduceFunction<Tuple2<Long, String>>) (t1, t2) -> {
                    int num1 = Integer.parseInt(t1.f1);
                    int num2 = Integer.parseInt(t2.f1);
                    int sum1 = num1 + num2;
                    Timestamp t = new Timestamp(System.currentTimeMillis());
                    return new Tuple2<>(t.getTime(), "" + sum1);
                }).returns(new TupleTypeInfo<>(Types.LONG,Types.STRING));
        DataStream<Row> rowDataStream = sum.map((MapFunction<Tuple2<Long, String>, Row>) rowData -> {
            Row row = new Row(2);
            row.setField(0, rowData.f0);
            row.setField(1, rowData.f1);
            return row;
        });
        FileSink<Row> textSink = FileSink.
                forRowFormat(new Path("home/vinay/window"),
                        (Encoder<Row>) (row,outputStream) -> {
                            outputStream.write('(');
                            assert row.getField(0) != null;
                            outputStream.write(row.getField(0).toString().getBytes());
                            outputStream.write(',');
                            assert row.getField(1) != null;
                            outputStream.write(row.getField(1).toString().getBytes());
                            outputStream.write(')');
                        })
                .build();

        rowDataStream.sinkTo(textSink);

        // execute program
        env.execute("Window");
    }
}
