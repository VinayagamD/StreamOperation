package org.vinaylogics.assigment;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
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
        );

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
                    trips1[7].equals("'null'") ? 0 : Integer.parseInt(trips1[7]) // numberOfPassengers
            );
        });

        // Step 2: Filter only ongoing trips with valid pickup and drop-off locations
        DataStream<Trip> ongoingTrips = trips.filter(trip -> trip.ongoing() && trip.dropOffLocation() != null);

        // 1. Popular destination
        KeyedStream<Tuple2<String, Integer>, String> destinations = ongoingTrips
                .map(trip -> new Tuple2<>(trip.dropOffLocation(), trip.numberOfPassengers()))
                .returns(new TupleTypeInfo<>(Types.STRING,Types.INT))
                .keyBy(tuple -> tuple.f0);

        DataStream<Tuple2<String, Integer>> totalPassengersPerDestination = destinations
                .reduce((ReduceFunction<Tuple2<String, Integer>>) (t1, t2) -> new Tuple2<>(t1.f0, t1.f1 + t2.f1))
                .returns(new TupleTypeInfo<>(Types.STRING,Types.INT));

        // Step 3: Calculate the average number of passengers from each pickup location
        KeyedStream<Tuple3<String, Integer, Integer>, String> pickupData = ongoingTrips
                .map(trip -> new Tuple3<>(trip.pickupLocation(), trip.numberOfPassengers(), 1))
                .returns(new TupleTypeInfo<>(Types.STRING,Types.INT,Types.INT))
                .keyBy(tuple -> tuple.f0);

        DataStream<Tuple2<String, Double>> averagePassengersPerPickup = pickupData
                .reduce((ReduceFunction<Tuple3<String, Integer, Integer>>)
                        (t1, t2) -> new Tuple3<>(t1.f0, t1.f1 + t2.f1, t1.f2 + t2.f2))
                .map(tuple -> new Tuple2<>(tuple.f0, (double) tuple.f1 / tuple.f2))
                .returns(new TupleTypeInfo<>(Types.STRING,Types.DOUBLE));

        // Step 4: Calculate the average number of trips for each driver
        KeyedStream<Tuple3<String, Integer, Integer>, String> driverData = ongoingTrips
                .map(trip -> new Tuple3<>(trip.driverName(), trip.numberOfPassengers(), 1))
                .returns(new TupleTypeInfo<>(Types.STRING,Types.INT,Types.INT))
                .keyBy(tuple -> tuple.f0);

        DataStream<Tuple2<String, Double>> averageTripsPerDriver = driverData
                .reduce((ReduceFunction<Tuple3<String, Integer, Integer>>) (t1, t2) ->
                        new Tuple3<>(t1.f0, t1.f1 + t2.f1, t1.f2 + t2.f2))
                .map(tuple -> new Tuple2<>(tuple.f0, (double) tuple.f1 / tuple.f2))
                .returns(new TupleTypeInfo<>(Types.STRING,Types.DOUBLE));

        // Print the results
        totalPassengersPerDestination.writeAsText("/home/vinay/popular_destinations");
        averagePassengersPerPickup.writeAsText("/home/vinay/Average_Passengers_per_Pickup");
        averageTripsPerDriver.writeAsText("/home/vinay/Average_Trips_per_Driver");

        env.execute("Trips Analytics");
    }
}
