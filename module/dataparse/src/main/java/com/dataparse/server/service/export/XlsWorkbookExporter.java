package com.dataparse.server.service.export;

import com.dataparse.server.service.upload.FileDescriptor;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.function.Consumer;

@Service
public class XlsWorkbookExporter extends AbstractXlsExporter {

    @Override
    public FileDescriptor export(List<ExportItemParams> exportItems,
                                 Consumer<Double> progressCallback, String datadocName) {
        Workbook wb = new XSSFWorkbook();
        double oneSheetPercentage = 1. / exportItems.size();
        for(ExportItemParams exportItem: exportItems) {
            exportSheet(exportItem, wb, d -> {
                int i = exportItems.indexOf(exportItem);
                progressCallback.accept(oneSheetPercentage * i + d / exportItems.size());
            });
        }
        try {
            return writeToFile(wb, datadocName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
