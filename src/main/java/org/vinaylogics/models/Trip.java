package org.vinaylogics.models;

public record Trip(
        String id,
        String licensePlate,
        String carType,
        String driverName,
        boolean ongoing,
        String pickupLocation,
        String dropOffLocation,
        int numberOfPassengers
) {
}
