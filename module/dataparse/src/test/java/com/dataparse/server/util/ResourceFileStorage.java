package com.dataparse.server.util;

import com.dataparse.server.service.storage.FileStorage;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.function.Consumer;

public class ResourceFileStorage implements FileStorage {

    @Override
    public InputStream getFile(String id) {
        return getClass().getClassLoader().getResourceAsStream(id);
    }

    @Override
    public String saveFile(InputStream is, String fileName) throws IOException {
        return null;
    }

    @Override
    public void removeFile(String id) {

    }

    @Override
    public String withNewFile(Consumer<BufferedWriter> out) throws IOException {
        return null;
    }

    @Override
    public String withNewOutputStream(Consumer<OutputStream> out) throws IOException {
        return null;
    }

    @Override
    public long getFileSize(String id) {
        return 0;
    }

    @Override
    public long getStorageSize() {
        return 0;
    }

    @Override
    public String getMd5(final String id) {
        return "";
    }
}
