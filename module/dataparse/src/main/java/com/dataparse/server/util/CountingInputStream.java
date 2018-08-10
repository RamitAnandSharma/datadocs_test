package com.dataparse.server.util;

import java.io.InputStream;
import java.util.function.Consumer;

public class CountingInputStream extends org.apache.commons.io.input.CountingInputStream {

    private Consumer<Integer> callback;

    public CountingInputStream(InputStream in, Consumer<Integer> callback) {
        super(in);
        this.callback = callback;
    }

    @Override
    protected synchronized void afterRead(int n) {
        super.afterRead(n);
        if(n != -1) {
            callback.accept(getCount());
        }
    }
}
