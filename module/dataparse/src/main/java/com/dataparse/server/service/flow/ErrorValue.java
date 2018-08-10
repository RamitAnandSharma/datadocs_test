package com.dataparse.server.service.flow;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class ErrorValue implements Serializable {

    private static final String TYPE = "ERROR";

    public ErrorValue(final String description) {
        this.description = description;
    }

    private long rowNumber;
    private String description;

    public String getType(){
        return TYPE;
    }

    public void setType(String type){
        // for jackson
    }

    @Override
    public String toString() {
        return (rowNumber == -1 ? "" : ("Row " + rowNumber + ": ")) + description;
    }
}
