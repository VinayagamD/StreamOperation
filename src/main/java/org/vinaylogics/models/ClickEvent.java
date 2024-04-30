package org.vinaylogics.models;

public record ClickEvent(
         String userId,
         String networkName,
         String userIp,
         String userCountry,
         String website,
         int timeSpent
) {
}
