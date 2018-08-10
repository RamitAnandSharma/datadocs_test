package com.dataparse.server.service.storage;

import com.dataparse.server.service.AbstractMongoRepository;
import com.mongodb.DBCollection;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;

import javax.annotation.PostConstruct;
import java.io.*;
import java.net.UnknownHostException;
import java.util.function.Consumer;

@Slf4j
public class GridFsFileStorage extends AbstractMongoRepository implements FileStorage {

    private DBCollection chunks;

    @PostConstruct
    public void init() {
        try {
            super.init();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
        chunks = database.getCollection("fs.chunks");
    }

    private GridFSDBFile getFileInner(String id) {
        GridFSDBFile file = gridFS.findOne(new ObjectId(id));
        if (file == null) {
            throw new RuntimeException("File " + id + " does not exist!");
        }
        return file;
    }

    @Override
    public InputStream getFile(String id) {
        return getFileInner(id).getInputStream();
    }

    @Override
    public String saveFile(InputStream is, String fileName) throws IOException {
        throw new UnsupportedOperationException("You can not create file with custom name in GRID FS.");
    }

    @Override
    public String saveFile(InputStream is) throws IOException {
        GridFSInputFile file = gridFS.createFile(is, true);
        ObjectId id = (ObjectId) file.getId();
        file.save();
        return id.toHexString();
    }

    @Override
    public String withNewFile(Consumer<BufferedWriter> out) throws IOException {
        GridFSInputFile file = gridFS.createFile();
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(file.getOutputStream()))) {
            out.accept(writer);
        } catch (Exception e) {
            throw new IOException(e);
        }
        ObjectId id = (ObjectId) file.getId();
        return id.toHexString();
    }

    @Override
    public String withNewOutputStream(Consumer<OutputStream> out) throws IOException {
        GridFSInputFile file = gridFS.createFile();
        try(OutputStream outputStream = new BufferedOutputStream(file.getOutputStream())) {
            out.accept(outputStream);
        }
        ObjectId id = (ObjectId) file.getId();
        return id.toHexString();
    }

    @Override
    public void removeFile(String id) {
        gridFS.remove(new ObjectId(id));
    }

    @Override
    public long getFileSize(String id) {
        return getFileInner(id).getLength();
    }

    @Override
    public long getStorageSize() {
        return (Integer) chunks.getStats().get("size");
    }

    @Override
    public String getMd5(final String id) {
        return getFileInner(id).getMD5();
    }
}
