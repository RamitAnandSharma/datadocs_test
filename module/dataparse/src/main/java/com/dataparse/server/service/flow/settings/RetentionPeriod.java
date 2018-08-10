package com.dataparse.server.service.flow.settings;

public enum RetentionPeriod {
    Hour(3600),
    Day(24 * 3600),
    Week(7 * 24 * 3600);
//        Month();

    private int s;

    RetentionPeriod(int s) {
        this.s = s;
    }

    public int getS() {
        return s;
    }
}
