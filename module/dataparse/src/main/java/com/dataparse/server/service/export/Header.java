package com.dataparse.server.service.export;

import com.dataparse.server.service.visualization.bookmark_state.state.*;
import com.dataparse.server.util.*;
import com.google.common.collect.*;
import lombok.*;

import java.util.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Header {

    private Show show;
    private List<Object> pivotKeys;

    public Header(Show show){
        this.show = show;
        this.pivotKeys = new ArrayList<>();
    }

    public String key(){
        return String.join("_", ImmutableList.<String>builder().addAll(ListUtils.toStringList(pivotKeys)).add(show.key()).build());
    }
}
