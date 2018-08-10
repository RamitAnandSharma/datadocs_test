package com.dataparse.server.service.visualization.bookmark_state.state;

import com.fasterxml.jackson.annotation.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class FilterValue implements Serializable{

    boolean selected;
    boolean show;
    Object key;
    Long docCount;

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FilterValue that = (FilterValue) o;

        if (selected != that.selected) {
            return false;
        }
        if (show != that.show) {
            return false;
        }
        return key != null ? key.equals(that.key) : that.key == null;
    }

    @Override
    public int hashCode() {
        int result = (selected ? 1 : 0);
        result = 31 * result + (show ? 1 : 0);
        result = 31 * result + (key != null ? key.hashCode() : 0);
        return result;
    }
}
