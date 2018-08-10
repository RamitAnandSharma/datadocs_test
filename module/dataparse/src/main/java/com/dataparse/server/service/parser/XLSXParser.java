package com.dataparse.server.service.parser;

import com.dataparse.server.service.storage.FileStorage;
import com.dataparse.server.service.upload.XlsFileDescriptor;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.IOException;
import java.io.InputStream;

public class XLSXParser extends AbstractXlsParser {

    public XLSXParser(FileStorage fileStorage, XlsFileDescriptor descriptor) {
        super(fileStorage, descriptor);
    }

    @Override
    public Workbook getWorkbook(InputStream is) throws IOException {
        return new XSSFWorkbook(is);
    }
}
