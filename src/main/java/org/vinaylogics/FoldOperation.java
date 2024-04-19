package org.vinaylogics;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Tuple4;
import scala.Tuple5;

public class FoldOperation {

    public static void main(String[] args) {
        // Checking input Parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> data = env.readTextFile(params.get("/home/vinay/avg"));

        // month, product, category, profit, count
//        DataStream<Tuple5<String,String, String, Integer, Integer>> mapped = data.map(new Splitter())
//                .returns(new TupleTypeInfo<>(Types.STRING,Types.STRING,Types.STRING,Types.INT,Types.INT));// tuple [June, Category5, Bat, 12, 1]

        // groupBy  'month'
//        DataStream<Tuple4<String, String, Integer, Integer>> folded = mapped.keyBy(0).

    }

    public static final class Splitter implements MapFunction<String, Tuple5<String, String, String, Integer, Integer>> {

        @Override
        public Tuple5<String, String, String, Integer, Integer> map(String value) throws Exception {
            var words = value.split(",");
            return new Tuple5<>(words[1], words[2], words[3], Integer.parseInt(words[4]), 1);
        }
    }
}
