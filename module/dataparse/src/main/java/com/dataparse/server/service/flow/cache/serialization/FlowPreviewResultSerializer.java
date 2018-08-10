package com.dataparse.server.service.flow.cache.serialization;

import com.dataparse.server.service.flow.cache.*;
import com.hazelcast.nio.*;
import com.hazelcast.nio.serialization.*;
import lombok.extern.slf4j.*;
import org.hibernate.internal.util.*;
import org.xerial.snappy.*;

import java.io.*;

@Slf4j
public class FlowPreviewResultSerializer implements StreamSerializer<FlowPreviewResultCacheValue> {

    @Override
    public int getTypeId() {
        return 12;
    }

    @Override
    public void write(ObjectDataOutput out, FlowPreviewResultCacheValue object) throws IOException {
        long start = System.currentTimeMillis();
        byte[] bytes = SerializationHelper.serialize(object);
        byte[] compressed = Snappy.compress(bytes);
        log.info("Compressed flow execution results in " + (System.currentTimeMillis() - start)
                 + ". Raw: {}; compressed: {}", bytes.length, compressed.length);
        out.write(compressed);
    }

    @Override
    public FlowPreviewResultCacheValue read(ObjectDataInput in) throws IOException {
        InputStream inputStream = (InputStream) in;
        return (FlowPreviewResultCacheValue) SerializationHelper.deserialize(new SnappyInputStream(inputStream));
    }

    @Override
    public void destroy() {
    }
}