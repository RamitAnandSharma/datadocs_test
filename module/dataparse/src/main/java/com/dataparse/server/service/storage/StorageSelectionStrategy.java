package com.dataparse.server.service.storage;

import avro.shaded.com.google.common.collect.*;
import com.dataparse.server.config.*;
import com.dataparse.server.service.parser.*;
import com.dataparse.server.service.upload.*;
import org.springframework.stereotype.*;

import java.util.*;

@Service
public class StorageSelectionStrategy {

    public static final Set<DataFormat> BQ_FORMATS =
            Sets.newHashSet(DataFormat.CSV, DataFormat.JSON_LINES, DataFormat.AVRO);

    public static final int ES_THRESHOLD_SIZE = 1024 * 1024 * 1024; // 1GB

    public StorageType getStorageType(Long fileSize, DataFormat dataFormat) {
        StorageStrategyType type = AppConfig.getStorageStrategyType();
        switch (type) {
            case ALWAYS_AWS_S3:
                return StorageType.AWS_S3;
            case ALWAYS_GCS:
                return StorageType.GCS;
            case ALWAYS_LOCAL:
                return StorageType.LOCAL;
            case DEPENDING_ON_FILE_SIZE:
                if(BQ_FORMATS.contains(dataFormat)) {
                    if(fileSize != null && fileSize > ES_THRESHOLD_SIZE) {
                        return StorageType.GCS;
                    } else {
                        return StorageType.AWS_S3;
                    }
                }
            case ALWAYS_GRID_FS:
                return StorageType.GRID_FS;
            default:
                throw new IllegalArgumentException("There is no storage type " + type + ". ");
        }
    }

    public void setFor(FileDescriptor descriptor){
        StorageType storageType = getStorageType(descriptor.getSize(), descriptor.getFormat());
        descriptor.setStorage(storageType);
    }

}
