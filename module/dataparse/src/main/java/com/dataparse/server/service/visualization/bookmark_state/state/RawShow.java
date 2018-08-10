package com.dataparse.server.service.visualization.bookmark_state.state;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.*;

import java.io.*;

@Getter @Setter
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class RawShow implements Serializable {

    public RawShow(String field){
        this.field = field;
    }

    String field;
    Sort sort;
    Boolean selected = false;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) return false;

        RawShow show = (RawShow) o;
        return field.equals(show.field);
    }

    @Override
    public int hashCode() {
        return field.hashCode();
    }
}
