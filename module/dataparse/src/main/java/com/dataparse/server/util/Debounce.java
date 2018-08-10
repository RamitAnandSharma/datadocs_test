package com.dataparse.server.util;

import java.util.function.Consumer;

public class Debounce<T> implements Consumer<T> {

    private Consumer<T> c;
    private volatile long lastCalled;
    private int interval;

    public Debounce(Consumer<T> c, int interval) {
        this.c = c;
        this.interval = interval;
    }

    @Override
    public void accept(T arg) {
        if( lastCalled + interval < System.currentTimeMillis() ) {
            lastCalled = System.currentTimeMillis();
            c.accept(arg);
        }
    }
}
