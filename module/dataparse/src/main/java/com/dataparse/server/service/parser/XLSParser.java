package com.dataparse.server.service.parser;

import com.dataparse.server.service.storage.FileStorage;
import com.dataparse.server.service.upload.XlsFileDescriptor;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Workbook;

import java.io.IOException;
import java.io.InputStream;

public class XLSParser extends AbstractXlsParser {

    public XLSParser(FileStorage fileStorage, XlsFileDescriptor descriptor) {
        super(fileStorage, descriptor);
    }

    @Override
    public Workbook getWorkbook(InputStream is) throws IOException {
        return new HSSFWorkbook(is);
    }
}
