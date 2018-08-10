package com.dataparse.server.service.storage.unifersal;

import javax.validation.constraints.NotNull;
import java.io.InputStream;
import java.util.Map;

public interface ISignedUrlStorage {
    Map<String, String> getSignedUploadUrls(int count);

    InputStream getFile(String fileName);

    void removeFile(@NotNull String fileName);
}
