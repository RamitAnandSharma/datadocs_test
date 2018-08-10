package com.dataparse.server.service.parser.exception;

import com.dataparse.server.service.parser.DataFormat;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class TooLargeFileException extends RuntimeException {
    private String fileName;
    private Long fileSize;
    private DataFormat dataFormat;

    @Override
    public String getMessage() {
        return String.format("To upload multiple json objects, we support json arrays. " +
                        "%s file '%s' is too large (%d bytes) for single json object.",
                dataFormat.options().name(), fileName, fileSize);
    }
}
