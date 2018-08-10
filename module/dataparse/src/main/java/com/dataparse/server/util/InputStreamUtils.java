package com.dataparse.server.util;

import org.apache.commons.io.*;
import org.apache.commons.io.input.*;

import java.io.*;

public class InputStreamUtils {

    public static BOMInputStream wrapWithoutBOM(InputStream is){
        return new BOMInputStream(is,
                                  ByteOrderMark.UTF_16LE,
                                  ByteOrderMark.UTF_16BE,
                                  ByteOrderMark.UTF_32LE,
                                  ByteOrderMark.UTF_32BE);
    }

    public static BufferingInputStream buffering(InputStream is){
        return new BufferingInputStream(is, 4096);
    }
}
