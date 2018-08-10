package com.dataparse.server.service.upload.preprocess;

import com.dataparse.server.service.parser.DataFormat;
import com.dataparse.server.service.upload.*;
import org.apache.commons.beanutils.BeanUtils;

import javax.validation.constraints.NotNull;
import java.lang.reflect.InvocationTargetException;

public class DescriptorFactory {
    public static Descriptor getDescriptorFromAnotherOne(@NotNull DataFormat format, @NotNull Descriptor descriptor) {
        Descriptor result = getDescriptor(format);
        try {
            BeanUtils.copyProperties(result, descriptor);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Can not copy properties from source descriptor.", e);
        }
        return result;
    }

    public static Descriptor getDescriptor(@NotNull DataFormat format) {
        switch (format) {
            case JSON_LINES:
            case JSON_OBJECT:
            case JSON_ARRAY:
            case AVRO:
                return new FileDescriptor();
            case CSV:
            case TSV:
                return new CsvFileDescriptor();
            case XLSX_SHEET:
            case XLS_SHEET:
            case XLS:
            case XLSX:
                return new XlsFileDescriptor();
            case XML:
                return new XmlFileDescriptor();
            case MYSQL_TABLE:
            case ORACLE_TABLE:
            case MSSQL_TABLE:
            case POSTGRESQL_TABLE:
                return new DbTableDescriptor();

            case MYSQL_QUERY:
            case POSTGRESQL_QUERY:
            case MSSQL_QUERY:
            case ORACLE_QUERY:
                return new DbQueryDescriptor();
            case COLLECTION_DELEGATE:
                return new CollectionDelegateDescriptor();
            case MSSQL:
            case MYSQL:
            case POSTGRESQL:
            case ORACLE:
                return new DbDescriptor();
            case COMPOSITE:
                return new CompositeDescriptor();
            case XLSB:
                throw new RuntimeException("Only xls and xlsx Excel files are supported.");
            case UNDEFINED:
                throw new IllegalArgumentException("Undefined descriptor type.");
            default:
                throw new IllegalArgumentException("There is no handler for " + format + " . Probably you forgot to add it, after implementing new feature.");
        }
    }
}
