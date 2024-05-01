package org.vinaylogics.state.operators.broadcast;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Map;

public class EmpCount {

    private static final MapStateDescriptor<String, String> excludeEmpDescriptor =
            new MapStateDescriptor<>("exclude_employ", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> excludeEmp = env.socketTextStream("localhost", 9090);

        BroadcastStream<String> excludeEmpBroadcast = excludeEmp.broadcast(excludeEmpDescriptor);

        // Define the path to the text file
        Path filePath = new Path("/home/vinay/broadcast.txt");

        FileSource<String> fileSource = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), filePath)
                .build();

        DataStream<Tuple2<String,Integer>> employees = env.fromSource(fileSource,
                WatermarkStrategy.noWatermarks(),
                "Employee Source"
        ).map((MapFunction<String, Tuple2<String, String>>)
                        value -> new Tuple2<>(value.split(",")[3], value))
                .returns(new TupleTypeInfo<>(Types.STRING,Types.STRING))
                .keyBy(f-> f.f0)
                        .connect(excludeEmpBroadcast)
                                .process(new ExcludeEmp()).returns(new TupleTypeInfo<>(Types.STRING,Types.INT));

        DataStream<Row> empRow = employees.map((MapFunction<Tuple2<String, Integer>, Row>) value -> {
            Row row = new Row(2);
            row.setField(0, value.f0);
            row.setField(1, value.f1);
            return row;
        });

        FileSink<Row> empSink = FileSink.forRowFormat(
                new Path("/home/vinay/bd_out"),
                (Encoder<Row>) (row, outputStream) -> {
                    outputStream.write('(');
                    outputStream.write(row.getField(0).toString().getBytes());
                    outputStream.write(',');
                    outputStream.write(row.getField(1).toString().getBytes());
                    outputStream.write(')');
                    outputStream.write('\n');
                }
        ).build();

        empRow.sinkTo(empSink);

        // Execute code
        env.execute("Broadcast Employee");
    }

    public static class ExcludeEmp extends KeyedBroadcastProcessFunction<String,Tuple2<String, String>, String, Tuple2<String, Integer>> {

        private transient ValueState<Integer> countState;

        @Override
        public void processElement(Tuple2<String, String> value, KeyedBroadcastProcessFunction<String, Tuple2<String, String>, String, Tuple2<String, Integer>>.ReadOnlyContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            Integer currCount = countState.value();

            // get card_id of current transaction
            final String cId = value.f1.split(",")[0];

            for (Map.Entry<String, String> cardEntry : ctx.getBroadcastState(excludeEmpDescriptor).immutableEntries()) {
                final String excludeId = cardEntry.getKey();
                if (cId.equals(excludeId)) {
                    return;
                }
            }
            countState.update(currCount + 1);
            out.collect(new Tuple2<>(value.f0, currCount+1));
        }


        @Override
        public void processBroadcastElement(String empData, KeyedBroadcastProcessFunction<String, Tuple2<String, String>, String, Tuple2<String, Integer>>.Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            String id = empData.split(",")[0];
            ctx.getBroadcastState(excludeEmpDescriptor).put(id, empData);
        }

        @Override
        public void open(OpenContext openContext) {
            ValueStateDescriptor<Integer> desc = new ValueStateDescriptor<>("", BasicTypeInfo.INT_TYPE_INFO,0);
            countState = getRuntimeContext().getState(desc);
        }
    }

}
