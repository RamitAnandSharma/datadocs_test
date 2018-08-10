package com.dataparse.server.service.visualization.bookmark_state.state;

import lombok.*;

import java.io.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class Agg implements Serializable {

    public Agg(String field, AggOp op){
        this.field = field;
        this.op = op;
    }

    public Agg(String field){
        this.field = field;
    }

    String field;
    AggOp op;

    AggSettings settings = new AggSettings();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) return false;
        if(!(o instanceof Agg)) return false;

        Agg agg = (Agg) o;
        return key().equals(agg.key());
    }

    @Override
    public int hashCode() {
        int result = field.hashCode();
        result = 31 * result + (op != null ? op.hashCode() : 0);
        return result;
    }

    public String key(){
        return (op != null ? op.name() + "_" : "") + field;
    }
}
