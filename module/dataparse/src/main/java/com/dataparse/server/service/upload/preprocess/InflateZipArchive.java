package com.dataparse.server.service.upload.preprocess;

import com.dataparse.server.service.parser.DataFormat;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

@Slf4j
@Service
public class InflateZipArchive implements IInflateArchive {

    public static final String MACOSX_ZIP_META_INFO = "__MACOSX/";

    private InputStream chooseInflateAlgorithm(InputStream is, DataFormat dataFormat, ZipEntry zipEntry) throws IOException {
        if(!formatSupported(dataFormat)) {
            throw new UnsupportedOperationException("We can not unzip file by format " + dataFormat.toString());
        }

        switch (dataFormat) {
            case ZIP:
                return setSeekToZipEntry(new ZipInputStream(is), zipEntry);
            case GZIP:
                return new GZIPInputStream(is);
            default:
                return null;
        }
    }

    @Override
    public InputStream inflate(InputStream is, DataFormat dataFormat, ZipEntry zipEntry) throws IOException {
        return chooseInflateAlgorithm(is, dataFormat, zipEntry);
    }

    @Override
    public Integer getFilesCount(InputStream is, DataFormat dataFormat) throws IOException {
        switch (dataFormat) {
            case ZIP:
                Integer count = 0;
                ZipInputStream zipInputStream = new ZipInputStream(is);
                ZipEntry zipEntry = zipInputStream.getNextEntry();

                while (zipEntry != null) {

                    if(!isMetaInfoEntry(zipEntry)) {
                        count++;
                    }

                    zipEntry = zipInputStream.getNextEntry();
                }

                return count;
            case GZIP:
                return 1;
            default:
                throw new UnsupportedOperationException("We can not unzip file by format " + dataFormat.toString());
        }
    }

    @Override
    public List<ZipEntry> getContainedFiles(InputStream inputStream, DataFormat dataFormat) throws IOException {
        List<ZipEntry> containedFiles = new ArrayList<>();

        switch (dataFormat) {
            case ZIP:
                ZipInputStream zipInputStream = new ZipInputStream(inputStream);
                ZipEntry zipEntry = zipInputStream.getNextEntry();

                while (zipEntry != null) {
                    if(!isMetaInfoEntry(zipEntry)) {
                        containedFiles.add(zipEntry);
                    }

                    zipEntry = zipInputStream.getNextEntry();
                }

                return containedFiles;

            case GZIP:
                containedFiles.add(new ZipEntry(""));
                return containedFiles;

            default:
                throw new UnsupportedOperationException("We can not unzip file by format " + dataFormat.toString());
        }
    }

    private Boolean isMetaInfoEntry(ZipEntry zipEntry) {
        return zipEntry.getName().startsWith(MACOSX_ZIP_META_INFO);
    }

    private InputStream setSeekToZipEntry(ZipInputStream inputStream, ZipEntry entry) throws IOException {
        ZipEntry current = inputStream.getNextEntry();

        while (!current.getName().equals(entry.getName())) {
            current = inputStream.getNextEntry();
        }

        return inputStream;
    }

}
