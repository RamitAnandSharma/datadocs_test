package com.dataparse.server.service.parser;

import com.dataparse.server.service.parser.jdbc.JDBCQueryParser;
import com.dataparse.server.service.parser.jdbc.JDBCTableParser;
import com.dataparse.server.service.storage.IStorageStrategy;
import com.dataparse.server.service.upload.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class ParserFactory {

    public final static int XLSX_THRESHOLD_SIZE = 1 * 1024 * 1024; // 10MB

    @Autowired
    private IStorageStrategy storageStrategy;

    @Autowired
    private AutowireCapableBeanFactory beanFactory;

    public Parser getParser(Descriptor descriptor) throws IOException {
        return getParser(descriptor, false);
    }

    public Parser getParser(Descriptor descriptor, Boolean forPreview) throws IOException {
        switch (descriptor.getFormat()) {
            case CSV:
                return new CSVParser(storageStrategy.get((FileDescriptor) descriptor), (CsvFileDescriptor) descriptor);
            case JSON_OBJECT:
            case JSON_ARRAY:
                return new JSONParser(storageStrategy.get((FileDescriptor) descriptor), (FileDescriptor) descriptor);
            case XLSX_SHEET:
                XlsFileDescriptor xlsFileDescriptor = (XlsFileDescriptor) descriptor;
                if(xlsFileDescriptor.getSize() > XLSX_THRESHOLD_SIZE){
                    return new XlsxStreamingParser(storageStrategy.get((FileDescriptor) descriptor), xlsFileDescriptor);
                } else {
                    return new XLSXParser(storageStrategy.get((FileDescriptor) descriptor), xlsFileDescriptor);
                }
            case XLS_SHEET:
                return new XLSParser(storageStrategy.get((FileDescriptor) descriptor), (XlsFileDescriptor) descriptor);
            case XML:
                return new XmlParser(storageStrategy.get((FileDescriptor) descriptor), (XmlFileDescriptor) descriptor);
            case JSON_LINES:
                return new JsonLineParser(storageStrategy.get((FileDescriptor) descriptor), (FileDescriptor) descriptor);
            case AVRO:
                return new AvroParser(storageStrategy.get((FileDescriptor) descriptor), (FileDescriptor) descriptor);
            case MYSQL_TABLE:
            case POSTGRESQL_TABLE:
            case MSSQL_TABLE:
            case ORACLE_TABLE:
//todo generalize
                JDBCTableParser jdbcTableParser = new JDBCTableParser((DbTableDescriptor) descriptor, forPreview);
                beanFactory.autowireBean(jdbcTableParser);
                return jdbcTableParser;
            case MYSQL_QUERY:
            case POSTGRESQL_QUERY:
            case MSSQL_QUERY:
            case ORACLE_QUERY:
//todo generalize
                DbQueryDescriptor dbQueryDescriptor = ((DbQueryDescriptor) descriptor);
                JDBCQueryParser jdbcQueryParser = new JDBCQueryParser(dbQueryDescriptor, dbQueryDescriptor.getQuery(), forPreview);
                beanFactory.autowireBean(jdbcQueryParser);
                return jdbcQueryParser;
            case COMPOSITE:
                CompositeDescriptor compositeDescriptor = ((CompositeDescriptor) descriptor);
                List<Parser> parsers = compositeDescriptor.getDescriptors().stream().map(d -> {
                    try {
                        return getParser(d);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }).collect(Collectors.toList());
                return new CompositeParser(compositeDescriptor, parsers);
            case COLLECTION_DELEGATE:
                // used with in-mem data cache and for test purposes
                return new CollectionDelegateParser((CollectionDelegateDescriptor) descriptor);
            default:
                throw new RuntimeException("Unknown upload file format: " + descriptor.getFormat());
        }
    }

}
