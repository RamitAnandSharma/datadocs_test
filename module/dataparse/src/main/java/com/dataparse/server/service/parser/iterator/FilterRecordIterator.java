package com.dataparse.server.service.parser.iterator;

import lombok.experimental.Delegate;

import java.util.Map;
import java.util.function.Consumer;

/**
 * A <code>FilterRecordIterator</code> contains
 * some other record iterator, which it uses as
 * its  basic source of data, possibly transforming
 * the data along the way or providing  additional
 * functionality. The class <code>FilterRecordIterator</code>
 * itself simply overrides all  methods of
 * <code>RecordIterator</code> with versions that
 * pass all requests to the contained record iterator.
 * Subclasses of <code>FilterRecordIterator</code>
 * may further override some of  these methods
 * and may also provide additional methods
 * and fields.
 */
public class FilterRecordIterator implements RecordIterator {

    @Delegate(excludes = RecordIteratorExclusion.class)
    protected RecordIterator it;

    public FilterRecordIterator(final RecordIterator it) {
        this.it = it;
    }

    public interface RecordIteratorExclusion {
        void remove();
        void forEachRemaining(Consumer<? super Map<String, Object>> action);
    }

}
