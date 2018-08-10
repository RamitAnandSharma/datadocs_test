package com.dataparse.server.controllers.api.table;

import com.dataparse.server.util.db.*;
import lombok.*;

@Data
public class GetDatadocsRequest {

    private int offset;
    private int limit;
    private OrderBy orderBy;
    private String path;

}
