package com.dataparse.server.service.parser.jdbc;

public enum Protocol {
    mysql, postgresql, sqlserver, oracle("thin");

    private String driverType;

    Protocol() {
        this(null);
    }

    Protocol(String driverType) {
        this.driverType = driverType;
    }

    public String getDriverType() {
        return driverType;
    }
}
