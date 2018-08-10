package com.dataparse.server.service.parser;

import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import com.dataparse.server.service.parser.iterator.RecordIterator;
import com.dataparse.server.util.MathUtils;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Slf4j
public abstract class Parser implements Closeable {

    public static final Set<String> POSSIBLE_NULL_VALUE = Sets.newHashSet("[blank]", "[blanks]");

    private final static int ESTIMATE_COUNT_OBJECT_LIMIT = 2000;
    private final static int SCHEMA_DEFINITION_ROWS_LIMIT = 1000;

    public Pair<Long, Boolean> getRowsEstimateCount(long fileSize) {
        Pair<Long, Boolean> result;
        Stopwatch stopwatch = Stopwatch.createStarted();
        long count = 0;
        try (RecordIterator it = parse()){
            while(it.hasNext() && count++ < ESTIMATE_COUNT_OBJECT_LIMIT){
                it.next();
            }
            if(count < ESTIMATE_COUNT_OBJECT_LIMIT){
                result = Pair.of(count, true);
            } else {
                // return estimated size
                result = Pair.of(MathUtils.roundToNextToLastDigitOnTheLeft(fileSize / (it.getBytesCount() / count)), false);
            }
            log.info("Define estimated rows count took {}", stopwatch.stop());
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Integer getMorePossibleHeaderIndex() {
        return 1;
    }

    public Integer getSkipRowsCountAfterHeader(Integer headerRow) {
        return 0;
    }

    abstract public RecordIterator parse() throws IOException;

    protected Map<AbstractParsedColumn, Object> postProcessWithNullBehaviour(Map<AbstractParsedColumn, Object> raw) {
        if(raw == null) {
            return null;
        }
        Map<AbstractParsedColumn, Object> result = new HashMap<>(raw);
        result.entrySet().forEach(field -> {
            if(field.getValue() != null && field.getValue() instanceof String && POSSIBLE_NULL_VALUE.contains((
                    StringUtils.strip(field.getValue().toString(), "\u0020\u00A0").toLowerCase()))) {
                field.setValue(null);
            }
        });
        return result;
    }

    @Override
    public void close() throws IOException {
        // used only for parsers like XLS that parse whole document in memory
    }
}
