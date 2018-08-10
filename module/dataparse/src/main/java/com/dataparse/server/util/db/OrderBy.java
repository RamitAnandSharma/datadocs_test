package com.dataparse.server.util.db;

import lombok.*;

@Data
public class OrderBy {

    private String field;
    private boolean desc;
    private String wrapFieldWith;

    public String toSqlString(){
        String field = wrapFieldWith == null ? this.field : String.format("%s(%s)", wrapFieldWith, this.field);
        return field + " " + (desc ? "DESC" : "ASC");
    }
}
