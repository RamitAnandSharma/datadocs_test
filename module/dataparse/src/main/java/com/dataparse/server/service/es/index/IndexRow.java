package com.dataparse.server.service.es.index;

import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class IndexRow {
    private Map<AbstractParsedColumn, Object> raw;
    /**
     * amount of bytes, that have been read until this element
     * */
    private Long rawBytes;
}
