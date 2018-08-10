package com.dataparse.server.controllers.api.flow;

import com.dataparse.server.service.db.DbQueryHistoryItem;
import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import com.dataparse.server.service.upload.Descriptor;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PreviewResponse {

    private Descriptor descriptor;
    private List<Map<AbstractParsedColumn, Object>> data;
    private Boolean cached;

    private List<DbQueryHistoryItem> newQueryHistoryItems;
    private int skippedCount;

}
