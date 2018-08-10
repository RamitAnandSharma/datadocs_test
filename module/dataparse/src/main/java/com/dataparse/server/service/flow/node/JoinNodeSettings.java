package com.dataparse.server.service.flow.node;

import com.dataparse.server.service.flow.api.JoinCondition;
import com.dataparse.server.service.flow.api.JoinType;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

@Data
@EqualsAndHashCode(callSuper = true)
public class JoinNodeSettings extends Settings {

    private JoinType type;
    private List<JoinCondition> conditions;

}
