package com.dataparse.server.service.files.preview;

import com.dataparse.server.service.parser.DataFormat;
import com.dataparse.server.service.parser.JsonLineParser;
import com.dataparse.server.service.parser.Parser;
import com.dataparse.server.service.parser.ParserFactory;
import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import com.dataparse.server.service.parser.iterator.RecordIterator;
import com.dataparse.server.service.parser.jdbc.JDBCQueryParser;
import com.dataparse.server.service.parser.type.ColumnInfo;
import com.dataparse.server.service.parser.type.TypeDescriptor;
import com.dataparse.server.service.storage.DefaultStorageStrategy;
import com.dataparse.server.service.storage.StorageType;
import com.dataparse.server.service.tasks.ExecutionException;
import com.dataparse.server.service.upload.ColumnDetector;
import com.dataparse.server.service.upload.Descriptor;
import com.dataparse.server.service.upload.FileDescriptor;
import com.dataparse.server.util.BeanUtils;
import com.google.common.base.Stopwatch;
import com.mysql.jdbc.exceptions.jdbc4.CommunicationsException;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.internal.util.SerializationHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
public class FilePreviewService {

    public static final int PREVIEW_LIMIT = 1000;

    @Autowired
    private ParserFactory parserFactory;

    @Autowired
    private DefaultStorageStrategy storageStrategy;

    public Descriptor copy(Descriptor descriptor, boolean detectSchema, List<Map<AbstractParsedColumn, Object>> rows) {
        Stopwatch timer = Stopwatch.createStarted();

        FileDescriptor result = new FileDescriptor();
        result.setStorage(StorageType.GRID_FS);
        // temporarily remove column info in order to copy contents "as is"
        List<ColumnInfo> tmpCols = descriptor.getColumns();
        descriptor.setColumns(null);
        descriptor.setLimit(PREVIEW_LIMIT);
        ColumnDetector columnDetector = new ColumnDetector();
        String storageKey = null;
        try {
            storageKey = storageStrategy.get(result).withNewFile((writer) -> {
                try {
                    for (Map<AbstractParsedColumn, Object> row : rows) {
                        writer.write(JsonLineParser.writeString(row));
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Can not write preview file.", e);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        descriptor.setLimit(null);
        descriptor.setColumns(tmpCols);
        result.setFormat(DataFormat.JSON_LINES);
        result.setPath(storageKey);
        result.setRowsExactCount((long) rows.size());
        result.setColumns(new ArrayList<>());
        if(detectSchema){
            List<ColumnInfo> columns = columnDetector.processRows(rows);
            result.setColumns(columns);
        } else {
            // copy schema from original source
            for (ColumnInfo column : descriptor.getColumns()) {
                ColumnInfo clonedColumn = (ColumnInfo) SerializationHelper.clone(column);
                BeanUtils.walk(descriptor, (k, v) -> "id".equals(k) ? null : v);
                result.getColumns().add(clonedColumn);
            }
        }
        log.info("Cached preview results in {}", timer.stop().elapsed(TimeUnit.MILLISECONDS));
        return result;
    }

    public Descriptor copy(Boolean preview, Descriptor descriptor, boolean detectSchema) {
        AtomicLong rowsCount = new AtomicLong();
        Stopwatch timer = Stopwatch.createStarted();
        List<Map<AbstractParsedColumn, Object>> rows = new ArrayList<>(PREVIEW_LIMIT);
        Integer oldLimit = descriptor.getLimit();
        if(preview) {
            descriptor.setLimit(PREVIEW_LIMIT);
        }

        try {
            Stopwatch stopwatch = Stopwatch.createStarted();
            try (Parser parser = parserFactory.getParser(descriptor, preview); RecordIterator it = parser.parse()) {
                Map<AbstractParsedColumn, Object> row;
                Map<AbstractParsedColumn, TypeDescriptor> possibleSchema = null;
                while ((row = it.next()) != null && rowsCount.incrementAndGet() < PREVIEW_LIMIT) {
                    if(rowsCount.get() == 1) {
                        possibleSchema = it.getSchema();
                    }
                    rows.add(row);
                }
                if(parser instanceof JDBCQueryParser && rowsCount.get() > 1) {
                    detectSchema = false;
                    ColumnDetector columnDetector = new ColumnDetector(possibleSchema);
                    descriptor.setColumns(columnDetector.getColumns());
                }
            }
            log.info("Retrieve {} rows took {}", rows.size(), stopwatch.stop());
        } catch (IOException e) {
            log.info("Failed to cached preview results, took {}", timer.stop().elapsed(TimeUnit.MILLISECONDS));
            if(e.getCause() instanceof CommunicationsException) {
                throw ExecutionException.of("query_exception", null);
            }
            throw new RuntimeException(e);
        }
        descriptor.setLimit(oldLimit);
        return copy(descriptor, detectSchema, rows);
    }
}
