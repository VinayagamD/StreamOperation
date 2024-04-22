package org.vinaylogics.models;

public record Trip(
        String id,
        String licensePlate,
        String carType,
        String driverName,
        boolean isTrip,
        String startLocation,
        String endLocation,
        int duration
) {
}
