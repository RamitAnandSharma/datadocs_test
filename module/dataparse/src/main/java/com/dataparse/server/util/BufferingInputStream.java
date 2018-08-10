package com.dataparse.server.util;

import java.io.*;

public class BufferingInputStream extends FilterInputStream {

    private CircularByteBuffer buffer;

    protected BufferingInputStream(final InputStream in, int size) {
        super(in);
        this.buffer = new CircularByteBuffer(size);
    }

    @Override
    public int read() throws IOException {
        int c = super.read();
        if(c != -1) {
            buffer.add((byte) (c & 0xff));
        }
        return c;
    }

    @Override
    public int read(final byte[] b) throws IOException {
        return super.read(b);
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        int c = super.read(b, off, len);
        for(int i = off; i< off+c; i++){
            buffer.add(b[i]);
        }
        return c;
    }

    public byte[] getBufferContents() throws IOException {
        return buffer.snapshot();
    }
}
