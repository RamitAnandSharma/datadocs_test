package com.dataparse.server.service.parser.column;

import com.dataparse.server.service.flow.settings.ColumnSettings;
import com.dataparse.server.service.parser.type.ColumnInfo;

public class ParsedColumnFactory {

    public static AbstractParsedColumn getByColumnInfo(ColumnInfo columnInfo) {
        String name = columnInfo.getSettings() != null && columnInfo.getSettings().getName() != null
                ? columnInfo.getSettings().getName()
                : columnInfo.getName();

        if(columnInfo.getInitialIndex() == null) {
            return new NamedParsedColumn(name);
        } else {
            return new IndexedParsedColumn(columnInfo.getInitialIndex(), name);
        }
    }

    public static AbstractParsedColumn getByColumnSettings(ColumnSettings columnSettings) {
        String name = columnSettings.getName();

        if(columnSettings.getInitialIndex() == null) {
            return new NamedParsedColumn(name);
        } else {
            return new IndexedParsedColumn(columnSettings.getInitialIndex(), name);
        }
    }

}
