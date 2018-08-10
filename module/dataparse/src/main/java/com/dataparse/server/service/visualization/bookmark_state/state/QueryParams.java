package com.dataparse.server.service.visualization.bookmark_state.state;

import com.dataparse.server.service.visualization.bookmark_state.filter.*;
import com.fasterxml.jackson.annotation.*;
import lombok.Data;
import org.hibernate.internal.util.SerializationHelper;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class QueryParams implements Serializable {

    List<Show> shows = new ArrayList<>();
    List<Agg> aggs = new ArrayList<>();
    List<Agg> pivot = new ArrayList<>();
    List<Filter> filters = new ArrayList<>();

    List<String> pivotOrder = new ArrayList<>();
    LimitParams limit = new LimitParams();

    String search = "";
    Row row;

    Boolean advancedMode = false;
    String advancedFilterQuery;

    @JsonIgnore
    public boolean isRaw(){
        return aggs.isEmpty() && pivot.isEmpty();
    }

    public QueryParams copy(){
        return (QueryParams) SerializationHelper.clone(this);
    }
}
