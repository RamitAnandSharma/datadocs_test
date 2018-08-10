package com.dataparse.server.coverage.utils.helpers;

import com.dataparse.server.service.storage.GDriveFileStorage;
import com.google.api.client.http.InputStreamContent;
import com.google.api.services.drive.model.File;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;

import java.io.IOException;
import java.io.InputStream;

@Slf4j
public class TestGDriveFileStorage extends GDriveFileStorage {

    @Override
    public String saveFile(InputStream is, String fileName) throws IOException {
        try {
            File fileMetadata = new File();
            fileMetadata.setName(fileName);
            File file = storage.files().create(fileMetadata, new InputStreamContent("application/octet-stream", is))
                    .setFields("id")
                    .execute();
            log.info("Uploaded to GD file ID: {}", file.getId());
            return file.getId();
        } catch (Throwable throwable) {
            Assert.fail("Failed to upload file to GD: " + throwable.getMessage());
        }

        return null;
    }
}
