package com.dataparse.server.service.flow.settings;

import com.dataparse.server.service.engine.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.*;

import java.io.*;

@Data
public class FlowSettings implements Serializable {

    private EngineSelectionStrategy engineSelectionStrategy = EngineSelectionStrategy.DEPENDING_ON_DATASET_SIZE;
    private IngestErrorMode ingestErrorMode = IngestErrorMode.REPLACE_WITH_NULL;

}
