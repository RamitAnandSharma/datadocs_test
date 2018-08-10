package com.dataparse.server.service.upload.preprocess;

import com.dataparse.server.service.parser.DataFormat;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Set;
import java.util.zip.ZipEntry;

public interface IInflateArchive {
    Set<DataFormat> SUPPORTED_FORMATS = Sets.newHashSet(DataFormat.ZIP, DataFormat.GZIP);

    InputStream inflate(InputStream file, DataFormat dataFormat, ZipEntry zipEntry) throws IOException;

    Integer getFilesCount(InputStream inputStream, DataFormat dataFormat) throws IOException;

    List<ZipEntry> getContainedFiles(InputStream inputStream, DataFormat dataFormat) throws IOException;

    default boolean formatSupported(DataFormat dataFormat) {
        return SUPPORTED_FORMATS.contains(dataFormat);
    }
}
