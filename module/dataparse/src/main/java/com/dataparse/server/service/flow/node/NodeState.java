package com.dataparse.server.service.flow.node;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class NodeState {

    private String nodeId;
    private NodeStateEnum state = NodeStateEnum.WAIT;
    private Double percentComplete;
    private Long processedRowsCount;
    private Long allRowsCount;

    public NodeState(String nodeId){
        this.nodeId = nodeId;
    }

}
