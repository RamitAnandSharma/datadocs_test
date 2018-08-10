package com.dataparse.server.service.storage;

import java.io.*;
import java.util.UUID;
import java.util.function.Consumer;

// todo move to filedescriptors?
public interface FileStorage {

    InputStream getFile(String id);

    default String saveFile(InputStream is) throws IOException {
        return this.saveFile(is, UUID.randomUUID().toString());
    }
    String saveFile(InputStream is, String fileName) throws IOException;

    String withNewFile(Consumer<BufferedWriter> out) throws IOException;

    String withNewOutputStream(Consumer<OutputStream> out) throws IOException;

    void removeFile(String id);

    long getFileSize(String id);

    long getStorageSize();

    String getMd5(String id);
}
