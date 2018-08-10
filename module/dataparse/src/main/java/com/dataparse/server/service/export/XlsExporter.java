package com.dataparse.server.service.export;

import com.dataparse.server.service.upload.FileDescriptor;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.function.Consumer;

@Service
public class XlsExporter extends AbstractXlsExporter {

    @Override
    public FileDescriptor export(List<ExportItemParams> exportItems,
                                 Consumer<Double> progressCallback, String datadocName) {
        if(exportItems.size() > 1){
            throw new RuntimeException("Can't export multiple bookmarks on single-page in XLS format.");
        }
        ExportItemParams exportItem = exportItems.get(0);
        XSSFWorkbook wb = new XSSFWorkbook();
        exportSheet(exportItem, wb, progressCallback);
        try {
            return writeToFile(wb, datadocName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
