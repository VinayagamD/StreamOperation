package org.vinaylogics;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.nio.charset.StandardCharsets;

public class SplitDemo {

    public static void main(String[] args) throws Exception {
        // Set up the stream execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checking the input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // Define the path to the text file
        Path filePath = new Path("/home/vinay/oddeven");

        FileSource<String> fileSource = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), filePath)
                .build();

        DataStream<String> text = env.fromSource(fileSource,
                WatermarkStrategy.noWatermarks(),
                "File Source"
        );

        // SplitStream<Integer> Since SplitStream is deprecated and removed trying the alternative approach
        OutputTag<Integer> evenTag  = new OutputTag<>("even", TypeInformation.of(Integer.class));
        OutputTag<Integer> oddTag  = new OutputTag<>("odd", TypeInformation.of(Integer.class));

        // Convert to integers and process to split into even and odd
        SingleOutputStreamOperator<Integer> mainStream = text
                .map((MapFunction<String, Integer>) Integer::parseInt)
                .process(new ProcessFunction<>() {
                    @Override
                    public void processElement(Integer value, ProcessFunction<Integer, Integer>.Context ctx, Collector<Integer> out) throws Exception {
                        if (value % 2 == 0) {
                            ctx.output(evenTag, value);
                        } else {
                            ctx.output(oddTag, value);
                        }

                    }
                });
        // Get the even and odd stream from side outputs
        DataStream<Integer> evenData = mainStream.getSideOutput(evenTag);
        DataStream<Integer> oddData = mainStream.getSideOutput(oddTag);

        // Write to text files
        evenData.writeAsText("/home/vinay/even");
        oddData.writeAsText("/home/vinay/odd");

        // execute program
        env.execute("ODD EVEN");
    }
}
