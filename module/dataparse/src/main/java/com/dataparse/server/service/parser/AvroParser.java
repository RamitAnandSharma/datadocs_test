package com.dataparse.server.service.parser;

import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import com.dataparse.server.service.parser.column.IndexedParsedColumn;
import com.dataparse.server.service.parser.iterator.RecordIterator;
import com.dataparse.server.service.parser.type.DataType;
import com.dataparse.server.service.parser.type.NumberTypeDescriptor;
import com.dataparse.server.service.parser.type.TypeDescriptor;
import com.dataparse.server.service.storage.FileStorage;
import com.dataparse.server.service.upload.FileDescriptor;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.util.Utf8;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

public class AvroParser extends Parser {

    private static final byte[] AVRO_HEADER = new byte[]{0x4f, 0x62, 0x6a, 0x01};

    public static boolean test(FileStorage fs, String path){
        try(InputStream is = fs.getFile(path);
            Reader reader = new InputStreamReader(is)){
            int i = 0, c;
            byte[] avroHeader = new byte[4];
            while ((c = reader.read()) != -1 && i < 4) {
                avroHeader[i++] = (byte) c;
            }
            return Arrays.equals(AVRO_HEADER, avroHeader);
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    private FileStorage fileStorage;
    private String path;

    public AvroParser(FileStorage fileStorage, FileDescriptor descriptor) {
        this.fileStorage = fileStorage;
        this.path = descriptor.getPath();
    }

    @Override
    public RecordIterator parse() throws IOException {
        DatumReader<GenericData.Record> datumReader = new GenericDatumReader<>();
        DataFileStream<GenericData.Record> stream = new DataFileStream<>(fileStorage.getFile(path), datumReader);
        return new RecordIterator() {

            GenericData.Record tmp = null;

            public Map<AbstractParsedColumn, Object> getRaw() {
                Schema schema = tmp.getSchema();
                Map<AbstractParsedColumn, Object> o = new LinkedHashMap<>();
                for(Schema.Field field : schema.getFields()){
                    String key = field.name();
                    int index = field.pos();
                    Object value = tmp.get(key);
                    if(value instanceof Utf8){
                        value = value.toString();
                    }
                    o.put(new IndexedParsedColumn(index, key), value);
                }
                return o;
            }

            @Override
            public Map<AbstractParsedColumn, TypeDescriptor> getSchema() {
                Map<AbstractParsedColumn, TypeDescriptor> result = new LinkedHashMap<>();
                Schema schema = tmp.getSchema();
                for(Schema.Field field : schema.getFields()){
                    if(field.schema().getType().equals(Schema.Type.NULL)){
                        continue;
                    }
                    int index = field.pos();
                    TypeDescriptor descriptor;
                    switch (field.schema().getType()){
                        case INT:
                        case LONG:
                            descriptor = new NumberTypeDescriptor(true, false);
                            break;
                        case BOOLEAN:
                        case STRING:
                            descriptor = new TypeDescriptor(DataType.STRING);
                            break;
                        case FLOAT:
                        case DOUBLE:
                            descriptor = new NumberTypeDescriptor();
                            break;
                        default:
                            continue; // just ignore nested types for the moment
                            // todo flatten?
                    }
                    result.put(new IndexedParsedColumn(index, field.name()), descriptor);
                }
                return result;
            }

            @Override
            public long getBytesCount() {
                return 0;
            }

            @Override
            public void close() throws IOException {
                stream.close();
            }

            @Override
            public boolean hasNext() {
                return stream.hasNext();
            }

            @Override
            public Map<AbstractParsedColumn, Object> next() {
                tmp = stream.next();
                if(!stream.getSchema().getType().equals(Schema.Type.RECORD)){
                    throw new IllegalStateException("File is expected to contain array of records");
                }
                return getRaw();
            }
        };
    }

}
