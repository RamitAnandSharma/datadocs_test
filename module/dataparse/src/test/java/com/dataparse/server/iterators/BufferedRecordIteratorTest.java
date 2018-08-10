package com.dataparse.server.iterators;

import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import com.dataparse.server.service.parser.column.NamedParsedColumn;
import com.dataparse.server.service.parser.iterator.BufferedRecordIterator;
import com.dataparse.server.service.parser.iterator.CollectionDelegateRecordIterator;
import com.dataparse.server.service.parser.iterator.RecordIterator;
import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@Slf4j
public class BufferedRecordIteratorTest {

    private Iterator<Map<AbstractParsedColumn, Object>> getNewRangeIterator(){
        List<Map<AbstractParsedColumn, Object>> list = new ArrayList<>();
        for(int i=0; i<2000; i++){
            list.add(ImmutableMap.of(new NamedParsedColumn("property"), i));
        }
        return list.iterator();
    }

    @Test
    public void testBufferedRecordIterator(){
        RecordIterator it = new CollectionDelegateRecordIterator(getNewRangeIterator());
        BufferedRecordIterator brit = new BufferedRecordIterator(it, null, null);
        int size = 0;
        while(brit.hasNext()){
            size++;
            log.debug("{}", brit.next());
        }
        assertEquals(2000, size);
    }

    @Test
    public void testBufferedRecordIteratorWithSkippedAfterStart(){
        RecordIterator it = new CollectionDelegateRecordIterator(getNewRangeIterator());
        BufferedRecordIterator brit = new BufferedRecordIterator(it, 500, null);
        int size = 0;
        while(brit.hasNext()){
            size++;
            log.info("{}", brit.next());
        }
        assertEquals(1500, size);
    }

    @Test
    public void testBufferedRecordIteratorWIthSkippedBeforeEnd(){
        RecordIterator it = new CollectionDelegateRecordIterator(getNewRangeIterator());
        BufferedRecordIterator brit = new BufferedRecordIterator(it, null, 500);
        int size = 0;
        while(brit.hasNext()){
            size++;
            log.debug("{}", brit.next());
        }
        assertEquals(1500, size);
    }

}
