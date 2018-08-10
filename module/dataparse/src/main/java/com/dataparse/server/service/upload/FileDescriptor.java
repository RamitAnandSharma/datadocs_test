package com.dataparse.server.service.upload;

import com.dataparse.server.service.storage.*;
import lombok.*;
import lombok.experimental.Builder;

import javax.persistence.*;

@Data
@Entity
@Builder
@Access(AccessType.FIELD)
@Table(name = "file_descriptor")
@NoArgsConstructor
@AllArgsConstructor
public class FileDescriptor extends Descriptor {
    private String path;
    private String contentType;
    private String originalFileName;
    private String extension;
    private String bufferPath;
    private String checksum;

    private Long size;
    private StorageType storage;

    public FileDescriptor(String path) {
        this.path = path;
    }

    public static FileDescriptor s3(){
        FileDescriptor descriptor = new FileDescriptor();
        descriptor.setStorage(StorageType.AWS_S3);
        return descriptor;
    }

    public static FileDescriptor gcs(){
        FileDescriptor descriptor = new FileDescriptor();
        descriptor.setStorage(StorageType.GCS);
        return descriptor;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }
}
