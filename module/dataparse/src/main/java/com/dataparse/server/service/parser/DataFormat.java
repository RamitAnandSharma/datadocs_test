package com.dataparse.server.service.parser;

import com.dataparse.server.service.storage.*;
import com.dataparse.server.service.upload.*;
import com.google.common.collect.ImmutableMap;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.*;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.*;

public enum DataFormat {
    JSON_OBJECT,
    JSON_ARRAY,
    CSV,
    TSV,
    XLS,
    XLS_SHEET,
    XLSX,
    XLSB,
    XLSX_SHEET,
    JSON_LINES,
    XML,

    ZIP,
    GZIP,

    MYSQL,
    MYSQL_TABLE,
    MYSQL_QUERY,

    POSTGRESQL,
    POSTGRESQL_TABLE,
    POSTGRESQL_QUERY,

    ORACLE,
    ORACLE_TABLE,
    ORACLE_QUERY,

    MSSQL,
    MSSQL_TABLE,
    MSSQL_QUERY,

    // composite descriptor that is used for union operations
    COMPOSITE,
    // this is for test purposes
    COLLECTION_DELEGATE,
    // for all other cases
    UNDEFINED,

    AVRO;

    private final static BiFunction<FileStorage, String, Boolean> ALWAYS_NO = (fs, s) -> false;
    
    public final static String CONTENT_TYPE_CSV = "text/csv";
    public final static String CONTENT_TYPE_JSON = "application/json";
    public final static String CONTENT_TYPE_XLS = "application/vnd.ms-excel";
    public final static String CONTENT_TYPE_XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
    public final static String CONTENT_TYPE_TEXT_XML = "text/xml";
    public final static String CONTENT_TYPE_APP_XML = "application/xml";
    public final static String CONTENT_TYPE_AVRO_BINARY = "avro/binary";
    public final static String CONTENT_TYPE_ZIP = "application/zip";


    @NoArgsConstructor
    @AllArgsConstructor
    public static class Options {
        String name;
        boolean composite;
        boolean section;
        BiFunction<FileStorage, String, Boolean> testFn;
        String extension;
        Boolean fileFormat;
        String contentType;
        Class<? extends Descriptor> descriptorClass;
        String keywords;
        @Getter
        long maxSizeInBytes;

        public String name() { return name; }

        public boolean isComposite() {
            return composite;
        }

        public boolean isSection() {
            return section;
        }

        public String keywords() { return keywords; }
    }

    public boolean testContents(FileStorage storage, String path) {
        return options().testFn.apply(storage, path);
    }

    public boolean isFileSizeAcceptable(Long size) {
        return size == null || size <= options().maxSizeInBytes;
    }

    public boolean testContentType(String contentType){
        if(StringUtils.isBlank(options().contentType)
                || StringUtils.isBlank(contentType)){
            return false;
        }
        return contentType.equalsIgnoreCase(options().contentType);
    }

    public static boolean isFileFormatAvailable(String fileName) {
        Optional<DataFormat> format = Arrays.stream(DataFormat.values()).filter(dataFormat -> dataFormat.testExtension(fileName)).findFirst();
        return format.isPresent() || StringUtils.isBlank(FilenameUtils.getExtension(fileName));
    }

    public static String getAvailableFileFormats() {
        return Arrays.stream(DataFormat.values())
                .filter(dataFormat -> dataFormat.options().fileFormat)
                .map(dataFormat -> dataFormat.options().extension)
                .distinct()
                .reduce((f1, f2) ->  f1 + "," + f2).get();
    }

    public boolean testExtension(String filename) {
        if (filename == null || options().extension == null) {
            return false;
        }
        String fileExt = FilenameUtils.getExtension(filename);
        return fileExt.equalsIgnoreCase(options().extension);
    }

    public Descriptor createDescriptor() {
        try {
            return options().descriptorClass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }

    private static final Map<DataFormat, Options> OPTIONS = ImmutableMap.<DataFormat, Options>builder()

            .put(ZIP, new Options("Zip", false, false, ALWAYS_NO, "zip", true, CONTENT_TYPE_ZIP, FileDescriptor.class, "zip", Long.MAX_VALUE))
            .put(GZIP, new Options("Gzip", false, false, ALWAYS_NO, "gz", true, CONTENT_TYPE_ZIP, FileDescriptor.class, "gzip gz", Long.MAX_VALUE))

            .put(JSON_OBJECT, new Options("JSON", false, false, JSONParser::testJSONObject, "json", true, CONTENT_TYPE_JSON, FileDescriptor.class, "json", 50000))
            .put(JSON_ARRAY, new Options("JSON", false, false, JSONParser::testJSONArray, "json", true, CONTENT_TYPE_JSON, FileDescriptor.class, "json", Long.MAX_VALUE))
            .put(CSV, new Options("CSV", false, false, CSVParser::testCSV, "csv", true, CONTENT_TYPE_CSV, CsvFileDescriptor.class, "csv", Long.MAX_VALUE))
            .put(TSV, new Options("TSV", false, false, CSVParser::testCSV, "tsv", true, CONTENT_TYPE_CSV, CsvFileDescriptor.class, "tsv", Long.MAX_VALUE))
            .put(XLS, new Options("Excel", true, false, ALWAYS_NO, "xls", true, CONTENT_TYPE_XLS, FileDescriptor.class, "excel xls spreadsheet", Long.MAX_VALUE))
            .put(XLSB, new Options("Excel", true, false, ALWAYS_NO, "xlsb", false, CONTENT_TYPE_XLS, FileDescriptor.class, "excel xls spreadsheet", Long.MAX_VALUE))
            .put(XLS_SHEET, new Options("Excel", false, true, ALWAYS_NO, null, false,null, XlsFileDescriptor.class, "excel xls spreadsheet", Long.MAX_VALUE))
            .put(XLSX, new Options("Excel", true, false, ALWAYS_NO, "xlsx", true, CONTENT_TYPE_XLSX, FileDescriptor.class, "excel xlsx spreadsheet", Long.MAX_VALUE))
            .put(XLSX_SHEET, new Options("Excel", false, true, ALWAYS_NO, null, false, null, XlsFileDescriptor.class, "excel xlsx spreadsheet", Long.MAX_VALUE))
            .put(JSON_LINES, new Options("JSON", false, false, ALWAYS_NO, "json", true, CONTENT_TYPE_JSON, FileDescriptor.class, "json", Long.MAX_VALUE))
            .put(XML, new Options("XML", false, false, ALWAYS_NO, "xml", true, CONTENT_TYPE_TEXT_XML, XmlFileDescriptor.class, "xml", Long.MAX_VALUE))
            .put(AVRO, new Options("Avro", false, false, AvroParser::test, "avro", true, CONTENT_TYPE_AVRO_BINARY, FileDescriptor.class, "avro", Long.MAX_VALUE))

            .put(MYSQL, new Options("MySQL", true, false, ALWAYS_NO, null, false, null, DbDescriptor.class, "mysql", Long.MAX_VALUE))
            .put(MYSQL_TABLE, new Options("MySQL", false, true, ALWAYS_NO, null, false, null, DbTableDescriptor.class, "mysql", Long.MAX_VALUE))
            .put(MYSQL_QUERY, new Options("MySQL", false, true, ALWAYS_NO, null, false, null,DbQueryDescriptor.class, "mysql", Long.MAX_VALUE))

            .put(POSTGRESQL, new Options("PostgreSQL", true, false, ALWAYS_NO, null, false, null, DbDescriptor.class, "postgres postgresql", Long.MAX_VALUE))
            .put(POSTGRESQL_TABLE, new Options("PostgreSQL", false, true, ALWAYS_NO, null, false, null, DbTableDescriptor.class, "postgres postgresql", Long.MAX_VALUE))
            .put(POSTGRESQL_QUERY, new Options("PostgreSQL", false, true, ALWAYS_NO, null, false, null, DbQueryDescriptor.class, "postgres postgresql", Long.MAX_VALUE))

            .put(ORACLE, new Options("Oracle", true, false, ALWAYS_NO, null, false, null, DbDescriptor.class, "oracle", Long.MAX_VALUE))
            .put(ORACLE_TABLE, new Options("Oracle", false, true, ALWAYS_NO, null, false, null, DbTableDescriptor.class, "oracle", Long.MAX_VALUE))
            .put(ORACLE_QUERY, new Options("Oracle", false, true, ALWAYS_NO, null, false, null, DbQueryDescriptor.class, "oracle", Long.MAX_VALUE))

            .put(MSSQL, new Options("SQL Server", true, false, ALWAYS_NO, null, false, null, DbDescriptor.class, "mssql sqlserver", Long.MAX_VALUE))
            .put(MSSQL_TABLE, new Options("SQL Server", false, true, ALWAYS_NO, null, false, null, DbTableDescriptor.class, "mssql sqlserver", Long.MAX_VALUE))
            .put(MSSQL_QUERY, new Options("SQL Server", false, true, ALWAYS_NO, null, false, null, DbQueryDescriptor.class, "mssql sqlserver", Long.MAX_VALUE))

            // composite descriptor that is used for union operations
            .put(COMPOSITE, new Options(null, true, false, ALWAYS_NO, null, false,null, CompositeDescriptor.class, null, Long.MAX_VALUE))
            // this is for test purposes
            .put(COLLECTION_DELEGATE, new Options(null, false, false, ALWAYS_NO, null, false, null, CollectionDelegateDescriptor.class, null, Long.MAX_VALUE))
            // for all other cases
            .put(UNDEFINED, new Options(null, false, false, ALWAYS_NO, null, false, null, FileDescriptor.class, null, Long.MAX_VALUE))
            .build();

    public Options options() {
        return OPTIONS.get(this);
    }

}
