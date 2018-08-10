package com.dataparse.server.service.parser.writer;

import java.io.*;
import java.util.*;

public interface RecordWriter extends AutoCloseable {

    void writeRecord(Map<String, Object> o) throws IOException;
    void flush() throws IOException ;

}
