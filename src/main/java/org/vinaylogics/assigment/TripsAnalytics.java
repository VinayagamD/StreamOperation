package org.vinaylogics.assigment;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.CsvOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;
import org.apache.flink.types.Row;
import org.vinaylogics.models.Trip;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;

public class TripsAnalytics {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Define the path to the text file
        Path filePath = new Path("/home/vinay/cab+flink.txt");

        FileSource<String> fileSource = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), filePath)
                .build();

        DataStream<String> inputData = env.fromSource(fileSource,
                WatermarkStrategy.noWatermarks(),
                "Cab Source"
        );;

        // Parse CSV data into Trip Objects
        DataStream<Trip> trips = inputData.map((MapFunction<String, Trip>) s -> {
            String[] trips1 = s.split(",");
            return new Trip(
                    trips1[0], // id
                    trips1[1], // license plate
                    trips1[2], // car type
                    trips1[3], // driver name
                    trips1[4].equals("yes"), // is it a trip?
                    trips1[5].equals("'null'") ? null : trips1[5], // start location
                    trips1[6].equals("'null'") ? null : trips1[6], // end location
                    trips1[7].equals("'null'") ? 0 : Integer.parseInt(trips1[7]) // duration
            );
        });

        // Filter for ongoing trips
        DataStream<Trip> ongoingTrips = trips.filter(Trip::isTrip);

        // Count trips per destination
        DataStream<Tuple2<String,Integer>> tripCounts = ongoingTrips
                .map(trip -> new Tuple2<>(trip.endLocation(),1)).returns(new TupleTypeInfo<>(Types.STRING, Types.INT))
                .keyBy(trip -> trip.f0) // partition by destination
                        .reduce((ReduceFunction<Tuple2<String, Integer>>) (t1, t2) ->  new Tuple2<>(t1.f0, t1.f1 + t2.f1))
                .returns(new TupleTypeInfo<>(Types.STRING, Types.INT));

        // Find the most popular Destination
        DataStream<Tuple2<String,Integer>> mostPopularDestination = tripCounts
                .keyBy(tuple -> 1) // Key by a constant to group all counts
                        .reduce((ReduceFunction<Tuple2<String, Integer>>) (t1, t2) -> t1.f1 > t2.f1 ? t1 : t2)
                .returns(new TupleTypeInfo<>(Types.STRING, Types.INT));

        // Convert Tuple2 to Row for CSV output
        DataStream<Row> rowDataStream = mostPopularDestination.map((MapFunction<Tuple2<String, Integer>, Row>) data -> {
            Row row = new Row(2);
            row.setField(0, data.f0);
            row.setField(1, data.f1);
            return row;
        });
        // Create a custom rolling policy
        CustomRollingPolicy rollingPolicy = new CustomRollingPolicy(
                Duration.ofMinutes(15), // Rollover interval
                Duration.ofMinutes(5), // Inactivity interval
                1024 * 1024 * 100 // Max part size (100 MB)
        );


        // Create a FileSink to write to CSV
        FileSink<Row> csvSink = FileSink
                .forRowFormat(
                        new Path("/home/vinay/most_popular_destination"),
                        (Encoder<Row>) (row, outputStream) -> {
                            assert row.getField(0) != null;
                            outputStream.write(row.getField(0).toString().getBytes());
                            outputStream.write(',');
                            outputStream.write(row.getField(1).toString().getBytes());
                            outputStream.write('\n');
                        })
                .build();

        // Sink the DataStream to the FileSink
        rowDataStream.sinkTo(csvSink);

        env.execute("Popular Destination");
    }
}
