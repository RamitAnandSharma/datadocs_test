package com.dataparse.server.service.flow;

import com.dataparse.server.service.flow.builder.*;
import com.dataparse.server.service.upload.*;

public class FlowGenerator {
    public static FlowContainerDTO singleSourceFlow(Upload upload, Long bookmarkId){
        return FlowBuilder.create()
                .withDataSource("INPUT_0", upload)
                .withOutput("OUTPUT", bookmarkId)
                .withLink("LINK_0", "INPUT_0", "OUTPUT")
                .build();
    }
}
