package com.dataparse.server.service.storage;

public enum StorageStrategyType {
    ALWAYS_LOCAL,
    ALWAYS_AWS_S3,
    ALWAYS_GCS,
    ALWAYS_GRID_FS,
    DEPENDING_ON_FILE_SIZE
}
