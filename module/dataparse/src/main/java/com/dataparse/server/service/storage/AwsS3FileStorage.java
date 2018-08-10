package com.dataparse.server.service.storage;

import alex.mojaki.s3upload.MultiPartOutputStream;
import alex.mojaki.s3upload.StreamTransferManager;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.IOUtils;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.PostConstruct;
import java.io.*;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;

@Slf4j
public class AwsS3FileStorage implements FileStorage
{
    private static final String STORAGE_BUCKET = "dataparse-storage";

    private static int numStreams = 1;
    private static int numUploadThreads = 1;
    private static int queueCapacity = 1;
    private static int partSize = 5;


    private AmazonS3 s3;

    private static String getAccessKey(){
        return Optional.of(System.getenv("AWS_ACCESS_KEY")).orElse("ACCESS_KEY");
    }

    private static String getSecretKey(){
        return Optional.of(System.getenv("AWS_SECRET_KEY")).orElse("SECRET_KEY");
    }

    @PostConstruct
    public void init(){
        try{
            s3 = new AmazonS3Client(new BasicAWSCredentials(getAccessKey(), getSecretKey()));
        } catch (Exception e)
        {
            throw new IllegalArgumentException("Wrong AWS S3 credentials.", e);
        }
    }

    private S3Object getFileInner(String id){
        S3Object object = s3.getObject(STORAGE_BUCKET, id);
        if(object == null){
            throw new RuntimeException("File " + id + " does not exist!");
        }
        return object;
    }

    @Override
    public InputStream getFile(String id) {
        S3Object object = getFileInner(id);
        return object.getObjectContent();
    }

    @Override
    public String saveFile(InputStream is, String fileName) throws IOException {
        final StreamTransferManager manager = new StreamTransferManager(STORAGE_BUCKET, fileName, s3, numStreams,
                numUploadThreads, queueCapacity, partSize, true);
        try (MultiPartOutputStream dst = manager.getMultiPartOutputStreams().get(0)) {
            IOUtils.copy(is, dst);
        } catch (Exception e) {
            manager.abort(e);
            throw new IOException(e);
        }
        manager.complete();
        return fileName;
    }

    @Override
    public String withNewFile(Consumer<BufferedWriter> out) throws IOException {
        String id = UUID.randomUUID().toString();
        final StreamTransferManager manager = new StreamTransferManager(STORAGE_BUCKET, id, s3, numStreams,
                numUploadThreads, queueCapacity, partSize, true);
        try (MultiPartOutputStream dst = manager.getMultiPartOutputStreams().get(0);
             OutputStreamWriter osw = new OutputStreamWriter(dst);
             BufferedWriter writer = new BufferedWriter(osw)) {
            out.accept(writer);
            dst.checkSize();
        } catch (Exception e) {
            manager.abort(e);
            throw new IOException(e);
        }
        manager.complete();
        return id;
    }

    @Override
    public String withNewOutputStream(Consumer<OutputStream> out) throws IOException {
        String id = UUID.randomUUID().toString();
        final StreamTransferManager manager = new StreamTransferManager(STORAGE_BUCKET, id, s3, numStreams,
                numUploadThreads, queueCapacity, partSize, true);
        try (OutputStream outputStream = manager.getMultiPartOutputStreams().get(0)) {
            out.accept(outputStream);
        } catch (Exception e) {
            manager.abort(e);
            throw new IOException(e);
        }
        manager.complete();
        return id;
    }

    @Override
    public void removeFile(String id) {
        s3.deleteObject(STORAGE_BUCKET, id);
    }

    @Override
    public long getFileSize(String id) {
        try(S3Object file = getFileInner(id)) {
            return file.getObjectMetadata().getContentLength();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getStorageSize() {
        long totalSize = 0;
        int totalItems = 0;
        ObjectListing objects = s3.listObjects(STORAGE_BUCKET);
        do {
            for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
                totalSize += objectSummary.getSize();
                totalItems++;
            }
            objects = s3.listNextBatchOfObjects(objects);
        } while (objects.isTruncated());
        log.debug("Amazon S3 bucket: " + STORAGE_BUCKET + " containing "
                + totalItems + " objects with a total size of " + totalSize
                + " bytes.");

        return totalSize;
    }

    @Override
    public String getMd5(String id) {
        return getFileInner(id).getObjectMetadata().getContentMD5();
    }
}