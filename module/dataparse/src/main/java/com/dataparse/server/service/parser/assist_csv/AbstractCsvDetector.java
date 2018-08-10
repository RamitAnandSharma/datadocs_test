package com.dataparse.server.service.parser.assist_csv;

import com.dataparse.server.service.storage.FileStorage;

abstract class AbstractCsvDetector {

    protected FileStorage fileStorage;
    protected String path;

    AbstractCsvDetector(FileStorage fileStorage, String path) {
        this.fileStorage = fileStorage;
        this.path = path;
    }

}
