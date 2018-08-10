package com.dataparse.server.service.visualization.bookmark_state.state;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.io.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Show implements Serializable {

    public Show(String field, Op op){
        this.field = field;
        this.op = op;
    }

    public Show(String field){
        this.field = field;
    }

    String field;
    Op op;

    ShowSettings settings = new ShowSettings();

    public String key(){
        return (op != null ? op.name() + "_" : "") + field;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) return false;

        Show show = (Show) o;

        if (!field.equals(show.field)) return false;
        return op == show.op;

    }

    @Override
    public int hashCode() {
        int result = field.hashCode();
        result = 31 * result + (op != null ? op.hashCode() : 0);
        return result;
    }
}
