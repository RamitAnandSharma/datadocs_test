package com.dataparse.server.service.storage;

import com.dataparse.server.util.SystemUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;

import javax.annotation.PostConstruct;
import java.io.*;
import java.util.UUID;
import java.util.function.Consumer;

@Slf4j
public class LocalFileStorage implements FileStorage {

    public static final String STORAGE_DIR_NAME = "STORAGE_DIR_NAME";

    private File storageDir;

    private static String getStorageDirName(){
        return SystemUtils.getProperty(STORAGE_DIR_NAME, "storage");
    }

    @PostConstruct
    public void init(){
        createStorageDirectory();
    }

    protected void createStorageDirectory(){
        String dirName = getStorageDirName();
        storageDir = new File(dirName);
        if(!storageDir.exists() && !storageDir.mkdir()) {
            throw new RuntimeException("Can't create storage");
        }
    }

    private File getFileInner(String id){
        File file = new File(storageDir, id);
        if(!file.exists()){
            throw new RuntimeException("File " + id + " does not exist!");
        }
        return file;
    }

    @Override
    public InputStream getFile(String id) {
        File file = getFileInner(id);
        try {
            return new FileInputStream(file);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String saveFile(InputStream is, String fileName) throws IOException {
        File f = new File(storageDir, fileName);
        try (FileOutputStream dst = new FileOutputStream(f)) {
            IOUtils.copy(is, dst);
        } catch (Exception e) {
            removeFile(fileName);
            throw new IOException(e);
        }
        return fileName;
    }

    @Override
    public String withNewFile(Consumer<BufferedWriter> out) throws IOException {
        String fileName = UUID.randomUUID().toString();
        File f = new File(storageDir, fileName);
        try (FileOutputStream dst = new FileOutputStream(f);
             OutputStreamWriter osw = new OutputStreamWriter(dst);
             BufferedWriter writer = new BufferedWriter(osw)) {
            out.accept(writer);
        } catch (Exception e) {
            throw new IOException(e);
        }
        return fileName;
    }

    @Override
    public String withNewOutputStream(Consumer<OutputStream> out) throws IOException {
        String fileName = UUID.randomUUID().toString();
        File f = new File(storageDir, fileName);
        try (OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(f))) {
            out.accept(outputStream);
        }
        return fileName;
    }

    @Override
    public void removeFile(String id) {
        File f = getFileInner(id);
        if(!f.delete()){
            throw new RuntimeException("Can't delete " + f.getName() + " from storage!");
        }
    }

    private long folderSize(File directory) {
        long length = 0;
        for (File file : directory.listFiles()) {
            if (file.isFile())
                length += file.length();
            else
                length += folderSize(file);
        }
        return length;
    }

    @Override
    public long getFileSize(String id) {
        File f = getFileInner(id);
        return f.length();
    }

    @Override
    public long getStorageSize(){
        return folderSize(storageDir);
    }

    @Override
    public String getMd5(final String id) {
        // maybe calculate md5 on insert and save as `${filename}-metadata`?
        File f = getFileInner(id);
        try (FileInputStream fis = new FileInputStream(f)) {
            return DigestUtils.md5Hex(fis);
        } catch (Exception e){
            throw new RuntimeException("Can't get MD5 hash for file " + id, e);
        }
    }
}
