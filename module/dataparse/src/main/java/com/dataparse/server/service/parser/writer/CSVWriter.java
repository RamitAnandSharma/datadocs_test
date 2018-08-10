package com.dataparse.server.service.parser.writer;

import com.univocity.parsers.common.processor.ObjectRowWriterProcessor;
import com.univocity.parsers.conversions.DateConversion;
import com.univocity.parsers.csv.*;

import java.io.*;
import java.util.*;


public class CSVWriter implements RecordWriter {

    private CsvWriter printer;
    private static final String DEFAULT_DATE_PATTERN = "yyyy-MM-dd HH:mm:ss.SSS";

    public CSVWriter(OutputStream outputStream, List<String> headers) throws IOException {
        CsvWriterSettings format = new CsvWriterSettings();
        ObjectRowWriterProcessor processor = new ObjectRowWriterProcessor();
        DateConversion dateConversion = new DateConversion(DEFAULT_DATE_PATTERN);
        processor.convertType(Date.class, dateConversion);

        format.setRowWriterProcessor(processor);
        format.getFormat().setLineSeparator("\n");
        this.printer = new CsvWriter(outputStream, format);
        this.printer.writeHeaders(headers.toArray(new String[headers.size()]));
    }

    @Override
    public void writeRecord(final Map<String, Object> o) throws IOException {
        printer.processRecord(o);
    }

    @Override
    public void flush() throws IOException {
        printer.flush();
    }

    @Override
    public void close() throws Exception {
        printer.close();
    }
}
