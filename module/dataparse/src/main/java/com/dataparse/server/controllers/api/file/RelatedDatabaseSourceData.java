package com.dataparse.server.controllers.api.file;

import com.dataparse.server.service.upload.RemoteLinkDescriptor;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Builder;


@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RelatedDatabaseSourceData {
    private String name;
    private RemoteLinkDescriptor query;
}
