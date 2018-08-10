package com.dataparse.server.service.storage.unifersal;

import com.dataparse.server.service.storage.GcsFileStorage;
import com.dataparse.server.util.SystemUtils;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.*;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.asynchttpclient.AsyncHttpClient;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.dataparse.server.config.AppConfig.BASE_URL;
import static org.asynchttpclient.Dsl.asyncHttpClient;




@Slf4j
@Service
@Primary
public class GcsPublicStorage implements PublicStorageInterface {

    private static final String GCS_PUBLIC_ACCOUNT = "GCS_PUBLIC_ACCOUNT";
    private static final String GCS_PUBLIC_BUCKET = "GCS_PUBLIC_BUCKET";
    private static final String CREDENTIALS_FOLDER = "credentials/";

    private static final String SIGNED_URL_RESUMABLE_HEADER = "x-goog-resumable";
    private static final String ORIGIN_HEADER = "Origin";
    private static final String SIGNED_URL_RESUMABLE_START = "start";
    private static final String CONTENT_LENGTH_HEADER = "Content-Length";
    private static final String LOCATION_HEADER = "Location";

    private static String getGcsPublicAccount(){
        return SystemUtils.getProperty(GCS_PUBLIC_ACCOUNT, "tidy-centaur-164320");
    }

    public static String getGcsPublicImageBucket(){
        return SystemUtils.getProperty(GCS_PUBLIC_BUCKET, "datadocs_images");
    }

    private Storage storage;
    private String bucketName;

    private AsyncHttpClient httpclient;

    @PostConstruct
    public void init(){
        try {
            String projectId = getGcsPublicAccount();
            String credentialsPath = CREDENTIALS_FOLDER + projectId + ".json";
            GoogleCredentials credentials = GoogleCredentials.fromStream(this.getClass().getClassLoader().getResourceAsStream(credentialsPath));

            this.storage = StorageOptions.newBuilder()
                    .setCredentials(credentials)
                    .setProjectId(projectId)
                    .build()
                    .getService();

            this.bucketName = getGcsPublicImageBucket();
            log.info("Initialized GCS public bucket {} for account {}", this.bucketName, projectId);

            this.httpclient = asyncHttpClient();
        } catch (IOException e){
            throw new RuntimeException("Can't initialize credentials for public storage", e);
        }
    }

    @PreDestroy
    public void destroy(){
        try {
            this.httpclient.close();
        } catch (IOException e) {
            log.error("Can't close http client", e);
        }
    }

    @Override
    public String uploadFile(InputStream is) {
        Acl acl = Acl.of(Acl.User.ofAllUsers(), Acl.Role.READER);

        UUID name = UUID.randomUUID();
        long start = System.currentTimeMillis();
        Bucket bucket = storage.get(bucketName);
        Blob blob = bucket.create(name.toString(), is);
        blob.createAcl(acl);
        log.info("Saving avatar {} took {}ms", name, System.currentTimeMillis() - start);
        return getFilePath(name.toString());
    }

    private CompletableFuture<Pair<String, String>> getResumableSessionUri(String filename, String url){
        return httpclient.preparePost(url)
                .addHeader(SIGNED_URL_RESUMABLE_HEADER, SIGNED_URL_RESUMABLE_START)
                .addHeader(CONTENT_LENGTH_HEADER, "0")
                .addHeader(ORIGIN_HEADER, BASE_URL)
                .execute()
                .toCompletableFuture()
                .thenApply(response -> {
                    if(response.getStatusCode() != 201){
                        throw new RuntimeException("Can't get signed URL for resumable uploads");
                    }
                    return Pair.of(filename, response.getHeader(LOCATION_HEADER));
                });
    }

    @Override
    public Map<String, String> getSignedUploadUrls(int count) {
        List<CompletableFuture<Pair<String, String>>> fileUrls = IntStream.range(0, count).mapToObj(i -> {
            String fileName = UUID.randomUUID().toString();
            Acl acl = Acl.of(Acl.User.ofAllUsers(), Acl.Role.WRITER);
            BlobInfo info = BlobInfo.newBuilder(GcsFileStorage.getGoogleCloudStorageBucketName(), fileName)
                    .setAcl(Lists.newArrayList(acl))
                    .setContentType("")
                    .build();
            Map<String, String> headersMap = ImmutableMap.of(SIGNED_URL_RESUMABLE_HEADER, SIGNED_URL_RESUMABLE_START);
            String url = storage.signUrl(info, 12, TimeUnit.HOURS, headersMap, Storage.SignUrlOption.httpMethod(HttpMethod.POST)).toString();
            return getResumableSessionUri(fileName, url);
        }).collect(Collectors.toList());
        CompletableFuture<Void> all = CompletableFuture.allOf(fileUrls.toArray(new CompletableFuture[fileUrls.size()]));
        try {
            Map<String, String> fileUrlsMap = all
                    .thenApply(v -> fileUrls.stream()
                            .map(CompletableFuture::join)
                            .collect(Collectors.toMap(Pair::getKey, Pair::getValue)))
                    .get();
            return fileUrlsMap;
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public InputStream getFile(String fileName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeFile(String fileName) {
        throw new UnsupportedOperationException();
    }

    private String getFilePath(String fileName) {
        return "https://storage.googleapis.com/" + getGcsPublicImageBucket() + "/" + fileName;
    }
}
