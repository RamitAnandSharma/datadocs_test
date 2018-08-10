package com.dataparse.server.service.parser.column;

import com.dataparse.server.util.JsonUtils;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = IndexedParsedColumn.class, name = "i"),
        @JsonSubTypes.Type(value = NamedParsedColumn.class, name = "n")
})
public abstract class AbstractParsedColumn implements Serializable {
    public static final String DELIMITER = "\u0011";

    private String columnName;

    @Override
    public String toString() {
        return JsonUtils.writeValue(this);
    }
}
