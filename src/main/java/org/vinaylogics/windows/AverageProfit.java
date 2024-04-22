package org.vinaylogics.windows;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;

public class AverageProfit {

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<String> data = env.socketTextStream("localhost", 9090);

                        // month, product, category, profit, count
        DataStream<Tuple5<String, String, String, Integer, Integer>> mapped = data.map(new Splitter())            //tuple [June, Category5, Bat, 12, 1]
                .returns(new TupleTypeInfo<>(Types.STRING,Types.STRING,Types.STRING,Types.INT, Types.INT));       //      [June, Category4, Perfume, 10, 1]

                        // groupBy 'month'
        DataStream<Tuple5<String, String, String, Integer, Integer>> reduced = mapped
                .keyBy(f -> f.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
                .reduce(new Reducer1());

        DataStream<Row> rowDataStream = reduced.map((MapFunction<Tuple5<String, String, String, Integer, Integer>, Row>) rowData -> {
           Row row = new Row(5);
           row.setField(0, rowData.f0);
           row.setField(1, rowData.f1);
           row.setField(2, rowData.f2);
           row.setField(3, rowData.f3);
           row.setField(4, rowData.f4);
           return row;
        });
        FileSink<Row> csvSink = FileSink.
                forRowFormat(new Path("home/vinay/www"),
                        (Encoder<Row>) (row,ouputStream)  -> {
                            assert row.getField(0) != null;
                            ouputStream.write('(');
                            assert row.getField(0) != null;
                            ouputStream.write(row.getField(0).toString().getBytes());
                            ouputStream.write(',');
                            ouputStream.write(row.getField(1).toString().getBytes());
                            ouputStream.write(',');
                            ouputStream.write(row.getField(2).toString().getBytes());
                            ouputStream.write(',');
                            ouputStream.write(row.getField(3).toString().getBytes());
                            ouputStream.write(',');
                            ouputStream.write(row.getField(4).toString().getBytes());
                            ouputStream.write(')');
                            ouputStream.write('\n');
                        }
                        ).build();


        rowDataStream.sinkTo(csvSink);

        // execute program
        env.execute("Avg Profit Per Month");
    }
    // *************************************************************************
    // USER FUNCTIONS                                                                                  // pre_result  = Category4,Perfume,22,2
    // *************************************************************************

    public static final class Reducer1 implements ReduceFunction<Tuple5<String, String, String, Integer, Integer>> {

        @Override
        public Tuple5<String, String, String, Integer, Integer> reduce(Tuple5<String, String, String, Integer, Integer> current, Tuple5<String, String, String, Integer, Integer> preResult) throws Exception {
            return new Tuple5<>(current.f0, current.f1, current.f2, current.f3+preResult.f3, current.f4+preResult.f4);
        }
    }


    public static class Splitter implements MapFunction<String, Tuple5<String, String, String, Integer, Integer>> {

        @Override
        public Tuple5<String, String, String, Integer, Integer> map(String value) { // 01-06-2018,June,Category5,Bat,12
            String[] words = value.split(",");  // words = [{01-06-2018},{June},{Category5},{Bat}.{12}
            // ignore timestamp, we don't need it for any calculations
            return new Tuple5<>(words[1],words[2],words[3],Integer.parseInt(words[4]),1);
            //    June    Category5      Bat                      12
        }
    }
}
