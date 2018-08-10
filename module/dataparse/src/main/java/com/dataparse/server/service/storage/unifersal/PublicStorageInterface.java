package com.dataparse.server.service.storage.unifersal;

import java.io.InputStream;

public interface PublicStorageInterface extends ISignedUrlStorage {
    String uploadFile(InputStream inputStream);
}