package com.dataparse.server.service.flow.settings;

import lombok.Data;

@Data
public class RetentionSettings {
    int value;
    RetentionPeriod period;

    public long toMillis() {
        return value * period.getS() * 1000;
    }
}
