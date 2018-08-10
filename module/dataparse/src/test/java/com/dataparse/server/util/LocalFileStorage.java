package com.dataparse.server.util;

import com.dataparse.server.service.storage.FileStorage;

import java.io.*;
import java.util.function.Consumer;

public class LocalFileStorage implements FileStorage {

    @Override
    public InputStream getFile(String id) {
        try {
            return new FileInputStream(id);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);

        }
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
