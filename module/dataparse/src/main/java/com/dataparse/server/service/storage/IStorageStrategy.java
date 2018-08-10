package com.dataparse.server.service.storage;

import com.dataparse.server.service.upload.*;

public interface IStorageStrategy {

    FileStorage get(StorageType storageType);

    FileStorage get(FileDescriptor descriptor);

    FileStorage getDefault();

}
