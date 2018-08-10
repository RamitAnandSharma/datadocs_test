package com.dataparse.server.service.export;

import com.dataparse.server.service.parser.iterator.ProgressAwareIterator;
import com.dataparse.server.service.parser.processor.Processor;
import com.dataparse.server.service.storage.*;
import com.dataparse.server.service.upload.FileDescriptor;
import com.dataparse.server.service.visualization.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import com.univocity.parsers.csv.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Service
public class CsvExporter extends Exporter {

    @Autowired
    private StorageSelectionStrategy storageSelectionStrategy;

    @Autowired
    private IStorageStrategy storageStrategy;

    @Autowired
    private VisualizationService visualizationService;

    @Override
    public FileDescriptor export(List<ExportItemParams> exportItems,
                                 Consumer<Double> progressCallback, String datadocName) {
        if(exportItems.size() > 1){
            throw new RuntimeException("Can't export multiple bookmarks in CSV format");
        }

        ExportItemParams exportItem = exportItems.get(0);
        ProgressAwareIterator<Map<String, Object>> it = visualizationService.getIterator(exportItem.getRequest(), exportItem.getState());
        QueryParams params = exportItem.getState().getQueryParams();

        List<String> aggKeys = params.getAggs().stream().map(Agg::key).collect(Collectors.toList());
        try {
            FileDescriptor descriptor = new FileDescriptor();
            descriptor.setOriginalFileName(getFileName(datadocName, "csv"));
            descriptor.setContentType("text/csv");
            storageSelectionStrategy.setFor(descriptor);
            String path = storageStrategy.get(descriptor).withNewFile((out) -> {
                CsvWriter printer = null;
                Tree.Node<Map<String, Object>> headers = it.getHeaders();
                List<List<String>> headerNames = getHeaderNames(headers, exportItem.getState(), exportItem.getProcessors());
                CsvWriterSettings format = new CsvWriterSettings();
                printer = new CsvWriter(out, format);
                if(params.getPivot().isEmpty()){
                    printer.writeHeaders(headerNames.get(0).toArray(new String[headerNames.get(0).size()]));
                } else {
                    for(List<String> lowLevelHeaders: headerNames){
                        printer.writeRow(lowLevelHeaders.toArray(new String[lowLevelHeaders.size()]));
                    }
                }
                Map<String, Object> grandTotalRowValues = null;
                if(it.hasNext() && !(params.getAggs().isEmpty() && params.getPivot().isEmpty())){
                    grandTotalRowValues = it.next(); // skip root row
                }
                Map<String, Show> headerKeys = getHeaderKeys(headers, params.getPivot(), params.getShows(), exportItem.getState().getPivotCollapsedState());
                int rowNumber = 0;
                while(it.hasNext() && (rowNumber++ < exportItem.getLimit())){
                    Map<String, Object> processed = it.next();
                    for(Processor processor : exportItem.getProcessors()){
                        processed = processor.process(processed, headerKeys);
                    }
                    List row = getRow(new ArrayList<>(headerKeys.keySet()), aggKeys, processed);
                    printer.writeRow(row);
                    progressCallback.accept(it.getComplete());
                }
                if(!params.getAggs().isEmpty() && params.getAggs().get(0).getSettings().getShowTotal()){
                    if(grandTotalRowValues == null){
                        grandTotalRowValues = new HashMap<>();
                    }
                    for(Processor processor : exportItem.getProcessors()){
                        grandTotalRowValues = processor.process(grandTotalRowValues, headerKeys);
                    }
                    List row = getRow(new ArrayList<>(headerKeys.keySet()), Collections.emptyList(), grandTotalRowValues);
                    row.add(0, GRAND_TOTAL_LABEL);
                    printer.writeRow(row);
                }
            });
            descriptor.setPath(path);
            return descriptor;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
