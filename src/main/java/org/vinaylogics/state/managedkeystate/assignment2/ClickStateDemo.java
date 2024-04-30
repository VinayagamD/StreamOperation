package org.vinaylogics.state.managedkeystate.assignment2;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;

public class ClickStateDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Define the path to the text file
        Path filePath = new Path("/home/vinay/ip-data.txt");
        FileSource<String> fileSource = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), filePath)
                .build();

        DataStream<String> data = env.fromSource(fileSource,
                WatermarkStrategy.noWatermarks(),
                "File Source"
        );

        DataStream<Tuple2<String,String>> keyedData = data.map((MapFunction<String, Tuple2<String, String>>) value -> {
            String[] words = value.split(",");
            return new Tuple2<>(words[4], value);
        }).returns(new TupleTypeInfo<>(Types.STRING,Types.STRING));

        // US click stream only
        DataStream<Tuple2<String,String>> usStream = keyedData.filter((FilterFunction<Tuple2<String, String>>) value -> {
            String country = value.f1.split(",")[3];
            return country.equals("US");
        }).returns(new TupleTypeInfo<>(Types.STRING,Types.STRING));

        // total number of clicks on every website in US
        DataStream<Tuple2<String,Integer>> clicksPerWebsite = usStream.map(
                (MapFunction<Tuple2<String, String>, Tuple3<String, String, Integer>>) value ->
                        new Tuple3<>(value.f0, value.f1, 1)).returns(new TupleTypeInfo<>(Types.STRING,Types.STRING,Types.INT))
                .keyBy(f -> f.f0)
                .window(TumblingProcessingTimeWindows.of(Duration.of(5, ChronoUnit.MILLIS)))
                .sum(2)
                .map((MapFunction<Tuple3<String, String, Integer>, Tuple2<String, Integer>>)
                        value -> new Tuple2<>(value.f0, value.f2))
                .returns(new TupleTypeInfo<>(Types.STRING,Types.INT));
        writeToFile(clicksPerWebsite,"home/vinay/clicks/clicksPerWebsite");


        // website with max clicks
        DataStream<Tuple2<String,Integer>> maxClicks = clicksPerWebsite
                .keyBy(f-> f.f0)
                        .window(TumblingProcessingTimeWindows.of(Duration.of(5, ChronoUnit.MILLIS)))
                                .maxBy(1);
        writeToFile(maxClicks,"home/vinay/clicks/maxClicks");

        // website with min clicks
        DataStream<Tuple2<String,Integer>> minClicks = clicksPerWebsite
                .keyBy(f-> f.f0)
                .window(TumblingProcessingTimeWindows.of(Duration.of(5, ChronoUnit.MILLIS)))
                .minBy(1);
        writeToFile(minClicks,"home/vinay/clicks/minClicks");

        // Avg Time To Website
        DataStream<Tuple2<String, Integer>> avgTimePerWebsite = usStream
                .map((MapFunction<Tuple2<String, String>, Tuple3<String, Integer, Integer>>) value -> {
                    int timeSpent = Integer.parseInt(value.f1.split(",")[5]);
                    return new Tuple3<>(value.f0, 1, timeSpent);
                }).returns(new TupleTypeInfo<>(Types.STRING, Types.INT, Types.INT))
                .keyBy(f-> f.f0)
                .window(TumblingProcessingTimeWindows.of(Duration.of(5,ChronoUnit.MILLIS)))
                .reduce((ReduceFunction<Tuple3<String, Integer, Integer>>)
                        (v1, v2) -> new Tuple3<>(v1.f0, v1.f1+v2.f1, v1.f2+v2.f2))
                .returns(new TupleTypeInfo<>(Types.STRING, Types.INT, Types.INT))
                .map((MapFunction<Tuple3<String, Integer, Integer>, Tuple2<String, Integer>>) value -> new Tuple2<>(value.f0, value.f2/value.f1))
                .returns(new TupleTypeInfo<>(Types.STRING, Types.INT));
        writeToFile(avgTimePerWebsite, "home/vinay/clicks/avgTimePerWebsite");

        // Distinct users on each website
        DataStream<Tuple2<String, Integer>> usersPerWebsite = usStream
                .keyBy(f-> f.f0)
                .flatMap(new DistinctUsers())
                .returns(new TupleTypeInfo<>(Types.STRING, Types.INT));

        writeToFile(usersPerWebsite, "home/vinay/clicks/usersPerWebsite");
        // Execute the data
        env.execute("Clicks Event Assignment");


    }

    public static class DistinctUsers extends RichFlatMapFunction<Tuple2<String, String>, Tuple2<String, Integer>> {
        private transient ListState<String> userState;

        @Override
        public void open(OpenContext openContext) throws Exception {
            ListStateDescriptor<String> desc = new ListStateDescriptor<>("user_state", BasicTypeInfo.STRING_TYPE_INFO);
            userState = getRuntimeContext().getListState(desc);
        }

        @Override
        public void flatMap(Tuple2<String, String> input, Collector<Tuple2<String, Integer>> out) throws Exception {
            userState.add(input.f1);
            HashSet<String> distinctUsers = new HashSet<>();
            userState.get().forEach(distinctUsers::add);
            out.collect(new Tuple2<>(input.f0, distinctUsers.size()));
        }
    }

    private static void writeToFile( DataStream<Tuple2<String,Integer>> dataStream, String file) {
        FileSink<Row> fileSink = FileSink.
                forRowFormat(new Path(file),
                        (Encoder<Row>) (row, outputStream)  -> {
                            outputStream.write('(');
                            outputStream.write(row.getField(0).toString().getBytes());
                            outputStream.write(',');
                            outputStream.write(row.getField(1).toString().getBytes());
                            outputStream.write(')');
                            outputStream.write('\n');
                        }
                ).build();
        convertToRow(dataStream).sinkTo(fileSink);
    }

    private static DataStream<Row> convertToRow(DataStream<Tuple2<String, Integer>> clicks) {
       return clicks.map((MapFunction<Tuple2<String,Integer>, Row>) value -> {
            Row row = new Row(2);
            row.setField(0, value.f0);
            row.setField(1, value.f1);
            return row;
        });
    }

}
