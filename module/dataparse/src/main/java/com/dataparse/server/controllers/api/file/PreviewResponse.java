package com.dataparse.server.controllers.api.file;

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

}