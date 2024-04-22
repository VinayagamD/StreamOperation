package org.vinaylogics.assigment;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.vinaylogics.models.Trip;

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

        // Output More Popular Destination
        mostPopularDestination.writeAsCsv("/home/vinay/most_popular_destination");

        env.execute("Popular Destination");
    }
}
