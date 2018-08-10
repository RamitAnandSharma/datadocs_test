package com.dataparse.server.service.storage;

import com.dataparse.server.config.*;
import com.dataparse.server.util.*;
import com.google.auth.oauth2.*;
import com.google.cloud.*;
import com.google.cloud.storage.*;
import com.google.common.base.*;
import lombok.extern.slf4j.*;

import javax.annotation.*;
import java.io.*;
import java.nio.channels.*;
import java.nio.file.NoSuchFileException;
import java.util.*;
import java.util.function.*;

@Slf4j
public class GcsFileStorage implements FileStorage {

    private static final String GOOGLE_CLOUD_STORAGE_ACCOUNT = "GOOGLE_CLOUD_STORAGE_ACCOUNT";
    private static final String GOOGLE_CLOUD_STORAGE_BUCKET = "GOOGLE_CLOUD_STORAGE_BUCKET";

    private static String getGoogleCloudStorageAccount(){
        return SystemUtils.getProperty(GOOGLE_CLOUD_STORAGE_ACCOUNT, "tidy-centaur-164320");
    }

    public static String getGoogleCloudStorageBucketName(){
        return SystemUtils.getProperty(GOOGLE_CLOUD_STORAGE_BUCKET, "dataparse_test");
    }

    private Storage storage;
    private String bucketName;

    @PostConstruct
    public void init(){
        try {
            String projectId = getGoogleCloudStorageAccount();
            File credentialsFile = new File(AppConfig.getGoogleAppCredentialsFolderPath() + "/"
                                             + projectId + ".json");
            GoogleCredentials credentials = GoogleCredentials.fromStream(
                    new FileInputStream(credentialsFile));

            this.storage = StorageOptions.newBuilder()
                    .setCredentials(credentials)
                    .setProjectId(projectId)
                    .build()
                    .getService();

            this.bucketName = getGoogleCloudStorageBucketName();
            log.info("Initialized GCS bucket {} in account {}", this.bucketName, projectId);
        } catch (IOException e){
            throw new RuntimeException("Can't initialize credentials", e);
        }
    }

    private Blob getFileInner(final String id) {
        return storage.get(bucketName).get(id);
    }

    @Override
    public InputStream getFile(final String id) {
        Blob blob = storage.get(bucketName).get(id);
        if(blob == null) {
            throw new RuntimeException(new NoSuchFileException(id));
        }
        return Channels.newInputStream(blob.reader());
    }

    @Override
    public String saveFile(final InputStream is, String fileName) throws IOException {
        long start = System.currentTimeMillis();
        storage.get(bucketName).create(fileName, is);
        log.info("Saving file {} took {}ms", fileName, System.currentTimeMillis() - start);
        return fileName;
    }

    @Override
    public String withNewFile(final Consumer<BufferedWriter> out) throws IOException {
        UUID uuid = UUID.randomUUID();
        WriteChannel channel = storage.writer(BlobInfo.newBuilder(bucketName, uuid.toString()).build());
        BufferedWriter writer = new BufferedWriter(Channels.newWriter(channel, Charsets.UTF_8.name()));
        out.accept(writer);
        writer.flush();
        writer.close();
        return uuid.toString();
    }

    @Override
    public String withNewOutputStream(Consumer<OutputStream> out) throws IOException {
        UUID uuid = UUID.randomUUID();
        WriteChannel channel = storage.writer(BlobInfo.newBuilder(bucketName, uuid.toString()).build());
        try(OutputStream outputStream = Channels.newOutputStream(channel)) {
            out.accept(outputStream);
        }
        return uuid.toString();
    }

    @Override
    public void removeFile(final String id) {
        storage.get(bucketName).get(id).delete();
    }

    @Override
    public long getFileSize(final String id) {
        return storage.get(bucketName).get(id).getSize();
    }

    @Override
    public long getStorageSize() {
        return 0;
    }

    @Override
    public String getMd5(final String id) {
        return getFileInner(id).getMd5();
    }
}
