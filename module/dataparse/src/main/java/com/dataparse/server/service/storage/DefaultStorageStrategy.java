package com.dataparse.server.service.storage;

import com.dataparse.server.config.*;
import com.dataparse.server.service.upload.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.*;

import javax.annotation.*;

@Service
public class DefaultStorageStrategy implements IStorageStrategy {

    @Autowired
    private GDriveFileStorage driveFileStorage;
    private AwsS3FileStorage awsS3FileStorage;
    private GcsFileStorage gcsFileStorage;
    private LocalFileStorage localFileStorage;
    private GridFsFileStorage gridFsFileStorage;
    private FileStorage defaultFileStorage;

    @PostConstruct
    public void init(){
        StorageStrategyType type = AppConfig.getStorageStrategyType();

        if(type.equals(StorageStrategyType.ALWAYS_AWS_S3) || type.equals(StorageStrategyType.DEPENDING_ON_FILE_SIZE)) {
            awsS3FileStorage = new AwsS3FileStorage();
            awsS3FileStorage.init();
            defaultFileStorage = awsS3FileStorage;
        }

        if(type.equals(StorageStrategyType.ALWAYS_GCS) || type.equals(StorageStrategyType.DEPENDING_ON_FILE_SIZE)) {
            gcsFileStorage = new GcsFileStorage();
            gcsFileStorage.init();
            defaultFileStorage = gcsFileStorage;
        }

        if(type.equals(StorageStrategyType.ALWAYS_LOCAL)) {
            localFileStorage = new LocalFileStorage();
            localFileStorage.init();
            defaultFileStorage = localFileStorage;
        }

        // always init GridFS storage because it's used in preview
        gridFsFileStorage = new GridFsFileStorage();
        gridFsFileStorage.init();
    }

    @Override
    public FileStorage get(final StorageType storageType){
        switch (storageType){
            case GCS:
                return gcsFileStorage;
            case GD:
                return driveFileStorage;
            case LOCAL:
                return localFileStorage;
            case GRID_FS:
                return gridFsFileStorage;
            case AWS_S3:
                return awsS3FileStorage;
            default:
                throw new RuntimeException("Unknown storage: " + storageType);
        }
    }

    @Override
    public FileStorage get(final FileDescriptor descriptor) {
        return get(descriptor.getStorage());
    }

    @Override
    public FileStorage getDefault() {
        return defaultFileStorage;
    }
}
