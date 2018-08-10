package com.dataparse.server.service.parser.writer;

import com.dataparse.server.service.parser.type.ColumnInfo;
import com.dataparse.server.service.parser.type.DataType;
import com.dataparse.server.util.FunctionUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

@Slf4j
public class AvroWriter implements RecordWriter {
    private static final String RECORD_BASE_NAME = "base";
    private DataFileWriter<GenericRecord> fileWriter;
    private Schema schema;

    public AvroWriter(OutputStream outputStream, List<ColumnInfo> columns) throws IOException {
        this.schema = buildSchema(columns);
        fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(this.schema));
        fileWriter.create(this.schema, outputStream);
    }

    @Override
    public void writeRecord(Map<String, Object> o) throws IOException {
        GenericRecord genericRecord = new GenericData.Record(schema);
        o.forEach(genericRecord::put);
        fileWriter.append(genericRecord);
    }

    private Schema buildSchema(List<ColumnInfo> columns) {
        SchemaBuilder.RecordBuilder<Schema> schema = SchemaBuilder.record(RECORD_BASE_NAME);
        SchemaBuilder.FieldAssembler<Schema> fieldsBuilder = schema.fields();
        columns.forEach(column -> {
            DataType dataType = column.getType().getDataType();
            boolean isArray = column.isRepeated();
            SchemaBuilder.BaseFieldTypeBuilder<Schema> columnSchema = fieldsBuilder.name(column.getAlias()).type().nullable();
            switch (dataType) {
                case DECIMAL:
                    if(isArray) {
                        columnSchema.array().items().nullable().doubleType().noDefault();
                    } else {
                        columnSchema.doubleType().noDefault();
                    }
                    break;
                case DATE:
                case TIME:
                    if(isArray) {
                        columnSchema.array().items().nullable().longType().noDefault();
                    } else {
                        columnSchema.longType().noDefault();
                    }
                    break;
                case STRING:
                    if(isArray) {
                        columnSchema.array().items().nullable().stringType().noDefault();
                    } else {
                        columnSchema.stringType().noDefault();
                    }
                    break;
                default:
                    log.info("No such field type: " + FunctionUtils.coalesce(dataType, "NULL"));
            }
        });
        return fieldsBuilder.endRecord();
    }

    public void flush() throws IOException {
        this.fileWriter.flush();
    }

    @Override
    public void close() throws Exception {
        this.fileWriter.close();
    }
}

