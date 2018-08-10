package com.dataparse.server.service.zip;

import org.zeroturnaround.zip.ZipEntrySource;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipEntry;

public class InputStreamSource implements ZipEntrySource {

    private final String path;
    private final InputStream is;
    private final long size;
    private final long time;
    private final int compressionLevel;

    public InputStreamSource(String path, InputStream is, long size) {
        this(path, is, System.currentTimeMillis(), size);
    }

    public InputStreamSource(String path, InputStream is, long size, long time) {
        this(path, is, time, size, -1);
    }
    public InputStreamSource(String path, InputStream is, long size, int compressionLevel) {
        this(path, is, System.currentTimeMillis(), size, compressionLevel);
    }

    public InputStreamSource(String path, InputStream is, long size, long time, int compressionLevel) {
        this.path = path;
        this.is = is;
        this.size = size;
        this.time = time;
        this.compressionLevel = compressionLevel;
    }

    public String getPath() {
        return path;
    }

    public ZipEntry getEntry() {
        ZipEntry entry = new ZipEntry(path);
        entry.setSize(size);
        if(compressionLevel != -1) {
            entry.setMethod(compressionLevel);
        }
        entry.setTime(time);
        return entry;
    }

    public InputStream getInputStream() throws IOException {
        return is;
    }

    public String toString() {
        return "InputStreamSource[" + path + "]";
    }

}

