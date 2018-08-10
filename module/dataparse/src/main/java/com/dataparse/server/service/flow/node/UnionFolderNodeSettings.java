package com.dataparse.server.service.flow.node;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class UnionFolderNodeSettings extends Settings {

    private Long folderId;

}
