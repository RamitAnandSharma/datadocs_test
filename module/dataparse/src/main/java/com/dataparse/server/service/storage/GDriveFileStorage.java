package com.dataparse.server.service.storage;

import com.dataparse.server.service.storage.unifersal.ISignedUrlStorage;
import com.github.rholder.retry.*;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.drive.model.FileList;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.IntStream;

@Slf4j
@Service
@Qualifier("GOOGLE_DRIVE")
public class GDriveFileStorage implements FileStorage, ISignedUrlStorage {
    public static final int DEFAULT_TIMEOUT = 120000;
    private GoogleCredential credentials;
    protected Drive storage;

    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

    private static HttpTransport HTTP_TRANSPORT;
    static {
        try {
            HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
        } catch (GeneralSecurityException | IOException e) {
            throw new RuntimeException("Initialization google drive service exception. ", e);
        }
    }

    @PostConstruct
    public void init() throws IOException {
        InputStream keyFile = this.getClass().getClassLoader().getResourceAsStream("credentials/Datadocs-24034b96ea2a.json");
        credentials = GoogleCredential.
                fromStream(keyFile).
                createScoped(Lists.newArrayList(DriveScopes.DRIVE_FILE));
        credentials.refreshToken();
        Drive.Builder builder = new Drive.Builder(HTTP_TRANSPORT, JSON_FACTORY, credentials)
                .setApplicationName("datadocs-163219");
        builder.setHttpRequestInitializer(request -> {
            credentials.initialize(request);
            request.setConnectTimeout(DEFAULT_TIMEOUT);
            request.setReadTimeout(DEFAULT_TIMEOUT);
        });
        storage = builder.build();
    }

    private void clearGoogleDriveStorage() throws IOException {
        FileList files = this.storage.files().list().execute();
        files.getFiles().stream().map(com.google.api.services.drive.model.File::getId).forEach(this::removeFile);
        if(files.size() > 0) {
            log.info("Removed next {} files", files.getFiles().size());
            clearGoogleDriveStorage();
        } else {
            log.info("GD cleanup finished.");
            return;
        }
    }

    @Override
    public Map<String, String> getSignedUploadUrls(int count) {
            Map<String, String> result = new HashMap<>(count, 1);
            try {
                credentials.refreshToken();
            } catch (IOException e) {
                log.error("Can not refresh access token ", e);
            }
            IntStream.range(0, count).asLongStream().forEach((v) -> {
                result.put(UUID.randomUUID().toString(), credentials.getAccessToken());
            });
            return result;
    }

    @Override
    public InputStream getFile(String id) {
        Retryer<InputStream> retryer = RetryerBuilder
                .<InputStream>newBuilder()
                .retryIfException()
                .withStopStrategy(StopStrategies.stopAfterAttempt(30))
                .withWaitStrategy(WaitStrategies.fixedWait(1000L, TimeUnit.MILLISECONDS))
                .build();
        try {
            return retryer.call(() -> storage.files().get(id).executeMediaAsInputStream());
        } catch (ExecutionException | RetryException e) {
            throw new RuntimeException("Can not retrieve file.", e);
        }
    }

    @Override
    public String saveFile(InputStream is, String fileName) throws IOException {
        throw new UnsupportedOperationException("Google drive storage doesn't support saving files.");
    }

    @Override
    public String withNewFile(Consumer<BufferedWriter> out) throws IOException {
        throw new UnsupportedOperationException("Google drive storage doesn't support saving files.");
    }

    @Override
    public String withNewOutputStream(Consumer<OutputStream> out) throws IOException {
        throw new UnsupportedOperationException("Google drive storage doesn't support saving files.");
    }

    @Override
    public void removeFile(String id) {
        try {
            storage.files().delete(id).execute();
        } catch (IOException e) {
            throw new RuntimeException("Can not remove file " + id, e);
        }
    }

    @Override
    public long getFileSize(String id) {
        try {
            return storage.files().get(id).setFields("size").execute().getSize();
        } catch (IOException e) {
            log.warn("Can not retrieve size for file {}", id, e);
            return 0;
        }
    }

    @Override
    public long getStorageSize() {
        return 0;
    }

    @Override
    public String getMd5(String id) {
        try {
            return storage.files().get(id).execute().getMd5Checksum();
        } catch (IOException e) {
            throw new RuntimeException("Can not retrieve checksum for file " + id, e);
        }
    }
}
