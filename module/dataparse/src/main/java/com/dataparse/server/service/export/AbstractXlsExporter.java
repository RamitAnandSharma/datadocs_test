package com.dataparse.server.service.export;

import com.dataparse.server.service.parser.iterator.*;
import com.dataparse.server.service.parser.processor.*;
import com.dataparse.server.service.storage.*;
import com.dataparse.server.service.upload.FileDescriptor;
import com.dataparse.server.service.visualization.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.usermodel.Row;
import org.springframework.beans.factory.annotation.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;
import java.util.stream.*;

public abstract class AbstractXlsExporter extends Exporter {

    private static final int MAX_ROWS_PER_SHEET = 1_048_576;

    @Autowired
    private StorageSelectionStrategy storageSelectionStrategy;

    @Autowired
    private IStorageStrategy storageStrategy;

    @Autowired
    private VisualizationService visualizationService;

    protected void exportSheet(ExportItemParams exportItem, Workbook wb, Consumer<Double> progressCallback){
        ProgressAwareIterator<Map<String, Object>> it =
                visualizationService.getIterator(exportItem.getRequest(), exportItem.getState());

        QueryParams params = exportItem.getState().getQueryParams();

        Sheet sheet = wb.createSheet(exportItem.getName());
        List<String> aggKeys = params.getAggs().stream().map(Agg::key).collect(Collectors.toList());

        if (it.hasNext() && !(params.getAggs().isEmpty() && params.getPivot().isEmpty())) {
            it.next(); // skip root rowNumber
        }
        int rowNumber = 0;

        Tree.Node<Map<String, Object>> headers = it.getHeaders();

        List<List<String>> headerNames = getHeaderNames(headers, exportItem.getState(),
                                                        exportItem.getProcessors());
        for (List<String> headersRow : headerNames) {
            org.apache.poi.ss.usermodel.Row headerRow = sheet.createRow(rowNumber++);
            for (int i = 0; i < headersRow.size(); i++) {
                Cell headerCell = headerRow.createCell(i);
                headerCell.setCellValue(headersRow.get(i));
            }
        }
        Map<String, Show> headerKeys = getHeaderKeys(headers, params.getPivot(), params.getShows(),
                                                     exportItem.getState().getPivotCollapsedState());

        int dataRowNumber = 0;
        while (it.hasNext() && rowNumber < MAX_ROWS_PER_SHEET && (dataRowNumber++ < exportItem.getLimit())) {
            Map<String, Object> processed = it.next();
            for (Processor processor : exportItem.getProcessors()) {
                processed = processor.process(processed, headerKeys);
            }
            List row = getRow(new ArrayList<>(headerKeys.keySet()), aggKeys, processed);
            writeRow(sheet, row, rowNumber++);
            progressCallback.accept(it.getComplete());
        }
        if (!params.getAggs().isEmpty() && params.getAggs().get(0).getSettings().getShowTotal()
                && rowNumber < MAX_ROWS_PER_SHEET) {
            Map<String, Object> grandTotalRowValues = it.getTotals();
            if(grandTotalRowValues == null){
                grandTotalRowValues = new HashMap<>();
            }
            for (Processor processor : exportItem.getProcessors()) {
                grandTotalRowValues = processor.process(grandTotalRowValues, headerKeys);
            }
            List row = getRow(new ArrayList<>(headerKeys.keySet()), Collections.emptyList(),
                              grandTotalRowValues);
            row.add(0, GRAND_TOTAL_LABEL);
            writeRow(sheet, row, rowNumber++);
        }
    }

    protected FileDescriptor writeToFile(Workbook workbook, String datadocName) throws Exception {
        AtomicReference<String> path = new AtomicReference<>();
        PipedInputStream in = new PipedInputStream(2048);
        PipedOutputStream out = new PipedOutputStream(in);
        FileDescriptor descriptor = new FileDescriptor();
        descriptor.setOriginalFileName(getFileName(datadocName, "xlsx"));
        descriptor.setContentType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
        storageSelectionStrategy.setFor(descriptor);
        Thread writer = new Thread(() -> {
            try {
                path.set(storageStrategy.get(descriptor).saveFile(in));
            } catch (IOException e) {
                throw new RuntimeException();
            }
        });
        writer.start();
        workbook.write(out);
        out.close();

        writer.join();
        in.close();

        descriptor.setPath(path.get());
        return descriptor;
    }

    protected Row writeRow(Sheet sheet, List rowObject, int rowNumber){
        org.apache.poi.ss.usermodel.Row dataRow = sheet.createRow(rowNumber);
        for(int i = 0; i < rowObject.size(); i++) {
            Cell cell = dataRow.createCell(i);
            Object v = rowObject.get(i);
            if(v != null) {
                if (v instanceof Number){
                    cell.setCellValue(((Number) v).doubleValue());
                } else if (v instanceof Date) {
                    cell.setCellValue((Date) v);
                } else if (v instanceof Boolean) {
                    cell.setCellValue((Boolean) v);
                } else {
                    cell.setCellValue(v.toString());
                }
            }
        }
        return dataRow;
    }

}
