package com.dataparse.server.service.visualization.bookmark_state.state;

import com.dataparse.server.service.flow.settings.*;
import lombok.Data;

import java.io.*;

@Data
public class ColSettings implements Serializable {

    private ColFormat format;
    private SearchType searchType = SearchType.EXACT_MATCH;

}
