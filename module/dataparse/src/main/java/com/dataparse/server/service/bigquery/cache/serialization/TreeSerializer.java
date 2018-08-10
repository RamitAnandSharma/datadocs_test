package com.dataparse.server.service.bigquery.cache.serialization;

import com.hazelcast.nio.*;
import com.hazelcast.nio.serialization.*;
import lombok.extern.slf4j.*;
import org.hibernate.internal.util.*;
import org.xerial.snappy.*;

import java.io.*;

@Slf4j
public class TreeSerializer implements StreamSerializer<BigQueryResult> {

    @Override
    public int getTypeId() {
        return 11;
    }

    @Override
    public void write(ObjectDataOutput out, BigQueryResult object) throws IOException {
        byte[] bytes = SerializationHelper.serialize(object);
        byte[] compressed = Snappy.compress(bytes);
        log.info("Compressed query results. Raw: {}; compressed: {}", bytes.length, compressed.length);
        out.write(compressed);
    }

    @Override
    public BigQueryResult read(ObjectDataInput in) throws IOException {
        InputStream inputStream = (InputStream) in;
        return (BigQueryResult) SerializationHelper.deserialize(new SnappyInputStream(inputStream));
    }

    @Override
    public void destroy() {
    }
}