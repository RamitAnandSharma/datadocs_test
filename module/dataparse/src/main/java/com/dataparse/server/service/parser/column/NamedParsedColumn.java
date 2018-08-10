package com.dataparse.server.service.parser.column;

import lombok.NoArgsConstructor;

@NoArgsConstructor
public class NamedParsedColumn extends AbstractParsedColumn {
    public NamedParsedColumn(String columnName) {
        super(columnName);
    }

}
