package com.dataparse.server.service.flow.convert;

import com.dataparse.server.service.flow.ErrorValue;
import com.dataparse.server.service.parser.DataFormat;
import com.dataparse.server.service.parser.ParserFactory;
import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import com.dataparse.server.service.parser.column.ParsedColumnFactory;
import com.dataparse.server.service.parser.iterator.RecordIterator;
import com.dataparse.server.service.parser.type.ColumnInfo;
import com.dataparse.server.service.parser.writer.AvroWriter;
import com.dataparse.server.service.parser.writer.CSVWriter;
import com.dataparse.server.service.parser.writer.JsonLineWriter;
import com.dataparse.server.service.parser.writer.RecordWriter;
import com.dataparse.server.service.storage.FileStorage;
import com.dataparse.server.service.storage.IStorageStrategy;
import com.dataparse.server.service.storage.StorageType;
import com.dataparse.server.service.upload.CsvFileDescriptor;
import com.dataparse.server.service.upload.Descriptor;
import com.dataparse.server.service.upload.FileDescriptor;
import com.dataparse.server.util.FunctionUtils;
import com.dataparse.server.util.ListUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.Charsets;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;


@Slf4j
@Service
public class ConvertService {

    private final static Boolean INCLUDE_NULL_ROWS = false;
    private static final DataFormat TARGET_FORMAT = DataFormat.AVRO;


    @Autowired
    private ParserFactory parserFactory;

    @Autowired
    @JsonIgnore
    private IStorageStrategy storageStrategy;

    public Descriptor convert(Descriptor source, StorageType storageType, Consumer<Double> progressCallback) {
        Stopwatch timer = Stopwatch.createStarted();

        String resultPath = null;
        FileStorage storage = storageStrategy.get(storageType);
        FileDescriptor resultDescriptor = new FileDescriptor();

        Map<AbstractParsedColumn, ColumnInfo> preset = ListUtils.groupByKey(source.getColumns()
                .stream()
                .filter(col -> !col.isRemoved())
                .collect(Collectors.toList()), ParsedColumnFactory::getByColumnInfo);

        try(RecordIterator it = parserFactory.getParser(source).parse()) {

            AtomicLong rowsCount = new AtomicLong();

            resultPath = storage.withNewOutputStream(out -> {
                try(RecordWriter writer = getWriter(source, TARGET_FORMAT, resultDescriptor, out)) {
                    Map<AbstractParsedColumn, Object> rowData;
                    while((rowData = it.next()) != null) {
                        Map<String, Object> convertedRow = fillRowData(rowData, preset);

                        if(!isNullsRow(convertedRow) || ConvertService.INCLUDE_NULL_ROWS) {
                            writer.writeRecord(convertedRow);
                            rowsCount.incrementAndGet();
                        }
                        if(it.getRowNumber() % 100000 == 0) {
                            log.info("Converted {}/{} rows", it.getRowNumber(), source.getRowsCount());
                            progressCallback.accept(it.getRowNumber() / (double) source.getRowsCount());
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            Long resultSize = storage.getFileSize(resultPath);
            resultDescriptor.setStorage(storageType);
            resultDescriptor.setFormat(TARGET_FORMAT);
            resultDescriptor.setPath(resultPath);
            resultDescriptor.setSize(resultSize);
            resultDescriptor.setColumns(source.getColumns());
            resultDescriptor.setRowsExactCount(rowsCount.get());
            log.info("File conversion {} took {} seconds.", resultPath, timer.stop().elapsed(TimeUnit.SECONDS));

            return resultDescriptor;
        } catch (Exception e) {
            if(resultPath != null) {
                storage.removeFile(resultPath);
            }
            throw new RuntimeException(e);
        }
    }

    // fill row data from raw entry and preset-map config
    private Map<String, Object> fillRowData(Map<AbstractParsedColumn, Object> source, Map<AbstractParsedColumn, ColumnInfo> preset) {
        Map<String, Object> convertedRow = new LinkedHashMap<>(preset.size());
        source.keySet().retainAll(preset.keySet());

        // convert to BQ required format
        for(Map.Entry<AbstractParsedColumn, Object> entry : source.entrySet()) {

            AbstractParsedColumn key = entry.getKey();
            ColumnInfo column = preset.get(key);
            String columnAlias = column.getAlias();
//   convert value to the list, for the cases where column is defined as list, but has a single column, mostly need to XML.
            Object value = column.isRepeated() && !(entry.getValue() instanceof Collection)
                    ? Lists.newArrayList(entry.getValue())
                    : entry.getValue();

            if(value == null || value.getClass() == ErrorValue.class) {
                convertedRow.put(columnAlias, null);
            } else {
                switch (column.getType().getDataType()) {
                    case STRING:
                        convertedRow.put(columnAlias, column.isRepeated() ? value : String.valueOf(value));
                        break;
                    case DECIMAL:
                        if(column.isRepeated()) {
                            Object result = ((List) value)
                                    .stream()
                                    .map(v -> v instanceof Number ? ((Number) v).doubleValue() : v)
                                    .collect(Collectors.toList());
                            convertedRow.put(columnAlias, result);
                        } else {
                            convertedRow.put(columnAlias, value);
                        }
                        break;
                    case TIME:
                    case DATE:
                        if(column.isRepeated()) {
                            convertedRow.put(columnAlias, ((List) value).stream().map(this::getDateValue).collect(Collectors.toList()));
                        } else {
                            convertedRow.put(columnAlias, getDateValue(value));
                        }
                        break;
                }
            }
        }

        return convertedRow;
    }

    private Long getDateValue(Object obj) {
        if(obj instanceof Date) {
            return ((Date) obj).getTime() * 1000;
        } else {
            log.warn("Can not process weird date. " + FunctionUtils.coalesce(obj, "NULL"));
            return null;
        }
    }

    private Boolean isNullsRow(Map<String, Object> row) {
        for(String key : row.keySet()) {
            if(row.get(key) != null) {
                return false;
            }
        }
        return true;
    }

    private RecordWriter getWriter(Descriptor source, DataFormat targetFormat, FileDescriptor resultDescriptor, OutputStream out) throws IOException {
        switch (targetFormat) {
            case CSV:
                CsvFileDescriptor csvDescriptor = (CsvFileDescriptor) resultDescriptor;
                csvDescriptor.getSettings().setUseHeaders(true);
                csvDescriptor.getSettings().setDelimiter(',');
                csvDescriptor.getSettings().setCharset(Charsets.UTF_8.name());
                List<String> headers = source.getColumns()
                        .stream()
                        .filter(col -> !col.isRemoved())
                        .map(ColumnInfo::getName)
                        .collect(Collectors.toList());
                return new CSVWriter(out, headers);
            case JSON_LINES:
                return new JsonLineWriter(out);
            case AVRO:
                return new AvroWriter(out, source.getColumns());
            default:
                throw new RuntimeException("Unsupported format: " + targetFormat);
        }
    }

}
