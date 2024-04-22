package org.vinaylogics.assigment;

import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;

public class CustomRollingPolicy implements RollingPolicy<Row, String> {
    private final Duration rolloverInterval; // Time-based rollover interval
    private final Duration inactivityInterval; // Inactivity-based rollover interval
    private final long maxPartSize; // Maximum file size before rollover

    public CustomRollingPolicy(Duration rolloverInterval, Duration inactivityInterval, long maxPartSize) {
        this.rolloverInterval = rolloverInterval;
        this.inactivityInterval = inactivityInterval;
        this.maxPartSize = maxPartSize;
    }

    @Override
    public boolean shouldRollOnCheckpoint(PartFileInfo<String> partFileInfo) throws IOException {
        // If the part is full, roll over on checkpoint
        return partFileInfo.getSize() >= maxPartSize;
    }

    @Override
    public boolean shouldRollOnEvent(PartFileInfo<String> partFileInfo, Row event) throws IOException {
        // Check if the current file size exceeds the maximum allowed size
        return partFileInfo.getSize() >= maxPartSize;
    }

    @Override
    public boolean shouldRollOnProcessingTime(PartFileInfo<String> partFileInfo, long currentTime) throws IOException {
        // Roll over if the current time exceeds the last modified time plus the rollover interval
        long lastModifiedTime = partFileInfo.getLastUpdateTime();
        return (currentTime - lastModifiedTime >= rolloverInterval.toMillis());
    }
}
