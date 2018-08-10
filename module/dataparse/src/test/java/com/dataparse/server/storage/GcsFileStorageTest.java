package com.dataparse.server.storage;

import com.dataparse.server.service.storage.*;
import com.dataparse.server.util.*;
import lombok.extern.slf4j.*;
import org.apache.commons.io.*;
import org.junit.*;

import java.io.*;

@Slf4j
public class GcsFileStorageTest {

    @Test
    @Ignore
    public void testStorage() throws Exception {
        ResourceFileStorage resourceStorage = new ResourceFileStorage();
        InputStream is = resourceStorage.getFile("test.csv");

        GcsFileStorage storage = new GcsFileStorage();
        storage.init();

        log.info("Uploading...");
        String key = storage.saveFile(is);
        log.info("Uploaded. Object size = {}", storage.getFileSize(key));

        log.info("Downloading...");
        is = storage.getFile(key);
        String contents = IOUtils.toString(is);
        log.info("Loaded from storage: {}", contents);

        log.info("Removing...");
        storage.removeFile(key);
        log.info("Removed.");
        try {
            storage.getFile(key);
            Assert.fail();
        } catch (Exception e){
            log.info("Removed file not found.");
        }
    }

}
